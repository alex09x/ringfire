//! Hold a reusable UDP port throughout each multicast test. Releasing a probe port
//! before mirror attachment lets an unrelated ephemeral sender take it under parallel tests.
use ringfire::replication::MulticastConfig;
use std::net::{Ipv4Addr, UdpSocket};
use std::os::fd::{AsRawFd, FromRawFd};
use std::time::Duration;

pub fn reserve(group: Ipv4Addr) -> (MulticastConfig, UdpSocket) {
    let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0) };
    assert!(fd >= 0, "UDP socket: {}", std::io::Error::last_os_error());
    let socket = unsafe { UdpSocket::from_raw_fd(fd) };
    let enabled: libc::c_int = 1;
    assert_eq!(
        unsafe {
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_REUSEADDR,
                (&enabled as *const libc::c_int).cast(),
                std::mem::size_of_val(&enabled) as libc::socklen_t,
            )
        },
        0
    );
    let address = libc::sockaddr_in {
        sin_family: libc::AF_INET as libc::sa_family_t,
        sin_port: 0,
        sin_addr: libc::in_addr {
            s_addr: libc::INADDR_ANY,
        },
        sin_zero: [0; 8],
    };
    assert_eq!(
        unsafe {
            libc::bind(
                socket.as_raw_fd(),
                (&address as *const libc::sockaddr_in).cast(),
                std::mem::size_of_val(&address) as libc::socklen_t,
            )
        },
        0,
        "UDP bind: {}",
        std::io::Error::last_os_error()
    );
    let cfg = MulticastConfig::new(group, socket.local_addr().unwrap().port());
    socket
        .join_multicast_v4(&group, &Ipv4Addr::UNSPECIFIED)
        .expect("multicast route is required");
    socket
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let sender = UdpSocket::bind("0.0.0.0:0").unwrap();
    sender.set_multicast_loop_v4(true).unwrap();
    assert_eq!(sender.send_to(b"probe", (group, cfg.port)).unwrap(), 5);
    let mut probe = [0; 8];
    let len = socket
        .recv(&mut probe)
        .expect("multicast loopback delivery is required");
    assert_eq!(&probe[..len], b"probe");
    socket
        .leave_multicast_v4(&group, &Ipv4Addr::UNSPECIFIED)
        .unwrap();
    (cfg, socket)
}
