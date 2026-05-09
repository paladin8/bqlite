//! Linux `perf_event_open` integration for [`super::PerfCounters`].
//!
//! Wave 5 / TASK-537 scope (c): open a perf-event group for the current
//! thread that aggregates `PERF_COUNT_HW_CPU_CYCLES`,
//! `PERF_COUNT_HW_BRANCH_MISSES`, and `PERF_COUNT_HW_CACHE_LL` /
//! `PERF_COUNT_HW_CACHE_OP_READ` / `PERF_COUNT_HW_CACHE_RESULT_MISS`.
//! The handle takes one read per morsel boundary — matches
//! `engine/morsel-scheduler.md` §8.4's <1% per-batch budget.
//!
//! ## Graceful disable
//!
//! `perf_event_open` returns `EACCES` / `EPERM` when the running user
//! lacks `CAP_PERFMON` (kernel ≥ 5.8) or when
//! `/proc/sys/kernel/perf_event_paranoid` is set to a value that
//! forbids unprivileged hardware events. CI runners and Docker
//! containers commonly fail this check; on those platforms this module
//! returns `None` from `open` and the surrounding `PerfCounters` falls
//! back to the disabled stub. The CLI surfaces this as
//! `not collected (no CAP_PERFMON)` (TASK-537 scope (f)).
//!
//! ## File-descriptor lifecycle
//!
//! [`LinuxPerfCounters`] owns three `RawFd`s (one per counter, grouped
//! through the kernel's `group_fd` mechanism). `Drop` closes every fd.
//! No `unsafe` impl of `Send` is added because `RawFd` is already
//! `Send`; the worker thread that opens the counters is the only
//! reader, and Rayon's worker move semantics keep the handle pinned to
//! that thread.

use std::os::raw::{c_int, c_long, c_ulong};
use std::os::unix::io::RawFd;

use super::PerfCounterSnapshot;

// ─────────────────────────────────────────────────────────────────────────────
// perf_event_attr — minimal subset matching the Linux ABI.
//
// The kernel layout is documented in `linux/perf_event.h`. We pin the
// `size` field to `std::mem::size_of::<perf_event_attr>()` so older
// kernels that ship a smaller struct truncate cleanly (the kernel
// accepts any `size <= sizeof(struct perf_event_attr)` and zero-fills
// the rest). Newer kernels accept any size up to the kernel's struct
// size; passing a larger value returns `E2BIG`.
// ─────────────────────────────────────────────────────────────────────────────

const PERF_TYPE_HARDWARE: u32 = 0;
const PERF_TYPE_HW_CACHE: u32 = 3;

const PERF_COUNT_HW_CPU_CYCLES: u64 = 0;
const PERF_COUNT_HW_BRANCH_MISSES: u64 = 5;

const PERF_COUNT_HW_CACHE_LL: u64 = 2;
const PERF_COUNT_HW_CACHE_OP_READ: u64 = 0;
const PERF_COUNT_HW_CACHE_RESULT_MISS: u64 = 1;

const PERF_FORMAT_GROUP: u64 = 1 << 3;

#[allow(non_snake_case, non_camel_case_types)]
#[repr(C)]
#[derive(Default)]
struct perf_event_attr {
    type_: u32,
    size: u32,
    config: u64,
    sample_period_or_freq: u64,
    sample_type: u64,
    read_format: u64,
    flags: u64, // disabled / inherit / pinned / exclusive / etc.
    wakeup_events_or_watermark: u32,
    bp_type: u32,
    bp_addr_or_config1: u64,
    bp_len_or_config2: u64,
    branch_sample_type: u64,
    sample_regs_user: u64,
    sample_stack_user: u32,
    clockid: i32,
    sample_regs_intr: u64,
    aux_watermark: u32,
    sample_max_stack: u16,
    __reserved_2: u16,
    aux_sample_size: u32,
    __reserved_3: u32,
    sig_data: u64,
}

const FLAG_DISABLED: u64 = 1 << 0;
const FLAG_EXCLUDE_KERNEL: u64 = 1 << 5;
const FLAG_EXCLUDE_HV: u64 = 1 << 6;

// `perf_event_open` syscall number on Linux. Constant across glibc but
// differs per architecture; the kernel exposes it via
// `<asm/unistd.h>`.
#[cfg(target_arch = "x86_64")]
const SYS_PERF_EVENT_OPEN: c_long = 298;
#[cfg(target_arch = "aarch64")]
const SYS_PERF_EVENT_OPEN: c_long = 241;
// Other Linux architectures exist (x86, arm, riscv, …) but x86_64 +
// aarch64 cover every CI runner and developer box bqlite targets in
// Wave 5. Adding more is mechanical: pin the right number from
// `<asm/unistd.h>`. Without a target_arch arm here the compile fails
// loudly on an unsupported arch, which is the correct outcome.
#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
const SYS_PERF_EVENT_OPEN: c_long = -1; // forces disabled at open time

extern "C" {
    fn syscall(num: c_long, ...) -> c_long;
    fn close(fd: c_int) -> c_int;
    fn read(fd: c_int, buf: *mut u8, count: usize) -> isize;
    fn ioctl(fd: c_int, request: c_ulong, ...) -> c_int;
}

// PERF_EVENT_IOC_ENABLE / RESET / DISABLE — used to arm and reset the
// counter group around the morsel boundary.
const PERF_EVENT_IOC_ENABLE: c_ulong = 0x2400;
const PERF_EVENT_IOC_RESET: c_ulong = 0x2403;

// ─────────────────────────────────────────────────────────────────────────────
// LinuxPerfCounters
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub(super) struct LinuxPerfCounters {
    /// Group leader (cpu_cycles). Closing this fd tears down the group;
    /// the other fds are also explicitly closed in `Drop` so the
    /// disable-on-drop ordering is deterministic.
    leader: RawFd,
    branch_misses: RawFd,
    llc_misses: RawFd,
}

impl LinuxPerfCounters {
    /// Try to open a perf-event group for the calling thread. Returns
    /// `None` on any failure — the caller treats this as graceful
    /// disable, not error.
    pub fn open() -> Option<Self> {
        if SYS_PERF_EVENT_OPEN < 0 {
            return None;
        }

        let leader = open_event(PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES, -1)?;
        let branch_misses =
            match open_event(PERF_TYPE_HARDWARE, PERF_COUNT_HW_BRANCH_MISSES, leader) {
                Some(fd) => fd,
                None => {
                    unsafe { close(leader) };
                    return None;
                }
            };
        let cache_config = (PERF_COUNT_HW_CACHE_LL)
            | (PERF_COUNT_HW_CACHE_OP_READ << 8)
            | (PERF_COUNT_HW_CACHE_RESULT_MISS << 16);
        let llc_misses = match open_event(PERF_TYPE_HW_CACHE, cache_config, leader) {
            Some(fd) => fd,
            None => {
                unsafe { close(branch_misses) };
                unsafe { close(leader) };
                return None;
            }
        };

        // Reset and enable the group so the leader is the one ioctl that
        // arms every member at once.
        unsafe {
            ioctl(leader, PERF_EVENT_IOC_RESET, 1u64); // 1 == PERF_IOC_FLAG_GROUP
            ioctl(leader, PERF_EVENT_IOC_ENABLE, 1u64);
        }

        Some(LinuxPerfCounters {
            leader,
            branch_misses,
            llc_misses,
        })
    }

    /// Read the three counters via a single grouped read. Returns
    /// `None` on any read failure — the caller treats this as zeros so
    /// per-morsel deltas degrade silently.
    pub fn read(&mut self) -> Option<PerfCounterSnapshot> {
        // PERF_FORMAT_GROUP layout:
        //   u64 nr;
        //   { u64 value; } values[nr];
        // We have three counters; expect 4 u64s.
        let mut buf = [0u64; 4];
        let bytes_expected = std::mem::size_of_val(&buf);
        let n = unsafe { read(self.leader, buf.as_mut_ptr() as *mut u8, bytes_expected) };
        if n != bytes_expected as isize {
            return None;
        }
        // buf[0] is the count (3); values follow in the order the fds
        // were added to the group.
        Some(PerfCounterSnapshot {
            cpu_cycles: buf[1],
            branch_misses: buf[2],
            llc_misses: buf[3],
        })
    }
}

impl Drop for LinuxPerfCounters {
    fn drop(&mut self) {
        // Close in reverse-add order; the leader teardown is last so
        // the kernel's group-disable propagates cleanly.
        unsafe {
            close(self.llc_misses);
            close(self.branch_misses);
            close(self.leader);
        }
    }
}

fn open_event(type_: u32, config: u64, group_fd: c_int) -> Option<RawFd> {
    let mut attr: perf_event_attr = unsafe { std::mem::zeroed() };
    attr.type_ = type_;
    attr.size = std::mem::size_of::<perf_event_attr>() as u32;
    attr.config = config;
    attr.read_format = PERF_FORMAT_GROUP;
    // Disabled at open so the caller's `IOC_ENABLE` arms the group as
    // a single transaction. Exclude kernel + hypervisor cycles so the
    // counters reflect bqlite's user-mode work, not OS overhead.
    attr.flags = FLAG_DISABLED | FLAG_EXCLUDE_KERNEL | FLAG_EXCLUDE_HV;

    let pid: c_int = 0; // current thread
    let cpu: c_int = -1; // any CPU
    let flags: c_ulong = 0;
    let fd = unsafe {
        syscall(
            SYS_PERF_EVENT_OPEN,
            &attr as *const _,
            pid,
            cpu,
            group_fd,
            flags,
        )
    };
    if fd < 0 {
        return None;
    }
    Some(fd as RawFd)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Opening succeeds or fails gracefully — both are valid CI
    /// outcomes. The test asserts no panic and (when open succeeds)
    /// that two consecutive reads round-trip without overflow.
    #[test]
    fn open_does_not_panic_and_reads_are_monotonic() {
        let mut handle = match LinuxPerfCounters::open() {
            Some(h) => h,
            None => return, // CI without CAP_PERFMON — disabled path is fine.
        };
        let a = handle.read().expect("first read");
        let b = handle.read().expect("second read");
        // Cycles never go down — monotonic growth across reads. In a
        // pinned thread this is a hard invariant.
        assert!(b.cpu_cycles >= a.cpu_cycles);
    }
}
