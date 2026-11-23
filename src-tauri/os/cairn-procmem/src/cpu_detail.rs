//! Native metadata for the handful of processes selected by a CPU window.
//! Enumeration and CPU counters belong to the shared census, not this detail read.

#[derive(Default)]
pub struct CpuDetail {
    pub command: Option<String>,
    pub nice: Option<i32>,
    pub scheduling_class: Option<String>,
    pub gaps: Vec<String>,
}

#[cfg(target_os = "linux")]
pub fn cpu_detail(pid: u32) -> CpuDetail {
    let mut out = CpuDetail::default();
    match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(stat) => {
            if let Some((_, tail)) = stat.rsplit_once(')') {
                out.nice = tail.split_whitespace().nth(16).and_then(|s| s.parse().ok());
            }
            out.command = stat
                .find('(')
                .zip(stat.rfind(')'))
                .map(|(start, end)| stat[start + 1..end].to_string());
        }
        Err(error) => out.gaps.push(format!("comm/nice: {error}")),
    }
    if out.nice.is_none() {
        out.gaps.push("nice: unreadable".into());
    }
    out
}

#[cfg(target_os = "macos")]
pub fn cpu_detail(pid: u32) -> CpuDetail {
    let mut out = CpuDetail::default();
    let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of_val(&info) as i32;
    // SAFETY: a caller-owned SDK struct with its exact size.
    if unsafe {
        libc::proc_pidinfo(
            pid as i32,
            libc::PROC_PIDTBSDINFO,
            0,
            (&mut info as *mut libc::proc_bsdinfo).cast(),
            size,
        )
    } == size
    {
        out.nice = Some(info.pbi_nice);
        let bytes: Vec<u8> = info
            .pbi_comm
            .iter()
            .map(|c| *c as u8)
            .take_while(|c| *c != 0)
            .collect();
        out.command = Some(String::from_utf8_lossy(&bytes).into_owned());
    } else {
        out.gaps
            .push(format!("comm/nice: {}", std::io::Error::last_os_error()));
    }
    out.gaps
        .push("QoS class: per-thread QoS is not exposed by the process query API".into());
    match task_category(pid) {
        Ok(category) => out.scheduling_class = Some(category),
        Err(gap) => out.gaps.push(gap),
    }
    out
}

/// Task category is the process-level scheduling role; individual threads can
/// override their QoS. A task-name port grants observation, never task control.
#[cfg(target_os = "macos")]
fn task_category(pid: u32) -> Result<String, String> {
    unsafe extern "C" {
        static mach_task_self_: u32;
        fn mach_port_deallocate(task: u32, port: u32) -> i32;
        fn task_name_for_pid(task: u32, pid: i32, name: *mut u32) -> i32;
        fn task_policy_get(
            task: u32,
            flavor: u32,
            policy: *mut i32,
            count: *mut u32,
            default: *mut i32,
        ) -> i32;
    }
    let mut port = 0;
    // SAFETY: the Mach call writes a single port name into the provided pointer.
    let status = unsafe { task_name_for_pid(mach_task_self_, pid as i32, &mut port) };
    if status != 0 {
        return Err(format!(
            "QoS task category: task_name_for_pid refused ({status}); thread QoS unavailable"
        ));
    }
    let (mut role, mut count, mut default) = (0, 1, 0);
    // TASK_CATEGORY_POLICY=1, count=sizeof(task_category_policy_data_t)/sizeof(integer_t).
    let status = unsafe { task_policy_get(port, 1, &mut role, &mut count, &mut default) };
    // SAFETY: release the send right acquired above on every result.
    unsafe {
        mach_port_deallocate(mach_task_self_, port);
    }
    if status != 0 {
        return Err(format!(
            "QoS task category: task_policy_get refused ({status}); thread QoS unavailable"
        ));
    }
    let name = match role {
        0 => "unspecified",
        1 => "foreground",
        2 => "background",
        3 => "control",
        4 => "graphics server",
        5 => "throttle",
        6 => "non-UI",
        7 => "default",
        8 => "Darwin background",
        9 => "user initiated",
        _ => "unknown",
    };
    Ok(format!("task category {name}; thread QoS varies"))
}

#[cfg(windows)]
pub fn cpu_detail(pid: u32) -> CpuDetail {
    use windows_sys::Win32::{
        Foundation::CloseHandle,
        System::Threading::{
            GetPriorityClass, OpenProcess, QueryFullProcessImageNameW,
            PROCESS_QUERY_LIMITED_INFORMATION,
        },
    };
    let mut out = CpuDetail::default();
    // SAFETY: query-only process handle, closed below on every successful open.
    let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if !handle.is_null() {
        let mut path = vec![0u16; 32768];
        let mut length = path.len() as u32;
        if unsafe { QueryFullProcessImageNameW(handle, 0, path.as_mut_ptr(), &mut length) } != 0 {
            let path = String::from_utf16_lossy(&path[..length as usize]);
            out.command = Some(path.rsplit(['\\', '/']).next().unwrap_or(&path).to_string());
        } else {
            out.gaps
                .push(format!("command: {}", std::io::Error::last_os_error()));
        }
        let priority = unsafe { GetPriorityClass(handle) };
        out.scheduling_class = match priority {
            0x40 => Some("idle"),
            0x4000 => Some("below normal"),
            0x20 => Some("normal"),
            0x8000 => Some("above normal"),
            0x80 => Some("high"),
            0x100 => Some("realtime"),
            _ => None,
        }
        .map(|name| format!("Windows priority class {name}"));
        if out.scheduling_class.is_none() {
            out.gaps.push(format!(
                "priority class: {}",
                std::io::Error::last_os_error()
            ));
        }
        unsafe {
            CloseHandle(handle);
        }
    } else {
        out.gaps.push(format!(
            "command: OpenProcess refused: {}",
            std::io::Error::last_os_error()
        ));
    }
    out.gaps
        .push("nice: Unix nice values do not exist on Windows".into());
    out
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
pub fn cpu_detail(_: u32) -> CpuDetail {
    CpuDetail {
        gaps: vec!["process metadata: unsupported platform".into()],
        ..Default::default()
    }
}
