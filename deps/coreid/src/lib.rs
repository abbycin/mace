#[cfg(target_os = "linux")]
use libc::{
    cpu_set_t, pthread_self, pthread_setaffinity_np, sched_getcpu, sysconf, CPU_SET,
    _SC_NPROCESSORS_ONLN,
};

#[cfg(target_os = "linux")]
pub fn current_core() -> usize {
    unsafe { sched_getcpu() as usize }
}

#[cfg(target_os = "linux")]
pub fn cores_online() -> usize {
    unsafe { sysconf(_SC_NPROCESSORS_ONLN) as usize }
}

#[cfg(target_os = "linux")]
pub fn bind_core(id: usize) {
    unsafe {
        let mut set: cpu_set_t = std::mem::zeroed();
        CPU_SET(id % cores_online(), &mut set);
        pthread_setaffinity_np(pthread_self(), size_of::<cpu_set_t>(), &set);
    }
}

#[cfg(target_os = "linux")]
pub fn unbind_core() {
    unsafe {
        let mut set: cpu_set_t = std::mem::zeroed();
        pthread_setaffinity_np(pthread_self(), std::mem::size_of::<cpu_set_t>(), &mut set);
    }
}

#[cfg(target_os = "linux")]
pub fn gettid() -> usize {
    unsafe { libc::gettid() as usize }
}
