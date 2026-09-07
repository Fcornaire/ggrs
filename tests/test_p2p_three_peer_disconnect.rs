//! Three peers, one drops, and the two survivors received a different amount of the dropped
//! player's last inputs. They must converge on the smaller frame or fail with an error
//! instead of panic.

use ggrs::{
    Config, Frame, GgrsError, GgrsEvent, GgrsRequest, InputStatus, Message, NonBlockingSocket,
    P2PSession, PlayerType, PredictRepeatLast, SessionBuilder, SessionState, UdpNonBlockingSocket,
};
use serde::{Deserialize, Serialize};
use serial_test::serial;
use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[repr(C)]
#[derive(Copy, Clone, PartialEq, Default, Serialize, Deserialize)]
struct Inp {
    v: u32,
}

struct Cfg;

impl Config for Cfg {
    type Input = Inp;
    type InputPredictor = PredictRepeatLast;
    type State = Frame;
    type Address = SocketAddr;
}

const DROPPED: usize = 1;
const PREDICTION_WINDOW: usize = 8;
const DISCONNECT_TIMEOUT: Duration = Duration::from_millis(400);

struct LossySocket {
    inner: UdpNonBlockingSocket,
    drop_to: Arc<Mutex<Option<SocketAddr>>>,
}

impl NonBlockingSocket<SocketAddr> for LossySocket {
    fn send_to(&mut self, msg: &Message, addr: &SocketAddr) {
        if *self.drop_to.lock().unwrap() == Some(*addr) {
            return;
        }
        self.inner.send_to(msg, addr);
    }

    fn receive_all_messages(&mut self) -> Vec<(SocketAddr, Message)> {
        self.inner.receive_all_messages()
    }
}

#[derive(Default)]
struct Game {
    frame: Frame,
    dropped_inputs: BTreeMap<Frame, (u32, InputStatus)>,
    saw_disconnect: bool,
}

impl Game {
    fn handle(&mut self, requests: Vec<GgrsRequest<Cfg>>) {
        for request in requests {
            match request {
                GgrsRequest::SaveGameState { cell, frame } => {
                    assert_eq!(frame, self.frame);
                    cell.save(frame, Some(self.frame), None);
                }
                GgrsRequest::LoadGameState { cell, frame } => {
                    self.frame = cell.load().unwrap();
                    assert_eq!(frame, self.frame);
                }
                GgrsRequest::AdvanceFrame { inputs } => {
                    self.dropped_inputs
                        .insert(self.frame, (inputs[DROPPED].0.v, inputs[DROPPED].1));
                    self.frame += 1;
                }
            }
        }
    }

    fn last_confirmed_dropped_frame(&self) -> Frame {
        self.dropped_inputs
            .iter()
            .filter(|(_, (_, status))| *status == InputStatus::Confirmed)
            .map(|(frame, _)| *frame)
            .max()
            .unwrap_or(-1)
    }
}

struct Peer {
    handle: usize,
    session: P2PSession<Cfg>,
    game: Game,
}

impl Peer {
    fn step(&mut self) -> Result<(), GgrsError> {
        let v = (self.game.frame as u32 + 1) * (self.handle as u32 + 1);
        self.session.add_local_input(self.handle, Inp { v })?;
        let advanced = self.session.advance_frame();
        for event in self.session.events() {
            if let GgrsEvent::Disconnected { .. } = event {
                self.game.saw_disconnect = true;
            }
        }
        self.game.handle(advanced?);
        Ok(())
    }
}

fn localhost(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
}

fn builder(rollback_window: Option<usize>) -> SessionBuilder<Cfg> {
    let mut builder = SessionBuilder::<Cfg>::new()
        .with_num_players(3)
        .unwrap()
        .with_max_prediction_window(PREDICTION_WINDOW)
        .with_disconnect_timeout(DISCONNECT_TIMEOUT)
        .with_disconnect_notify_delay(Duration::from_millis(100));
    if let Some(window) = rollback_window {
        builder = builder.with_max_rollback_window(window);
    }
    builder
}

fn add_players(builder: SessionBuilder<Cfg>, local: usize, ports: [u16; 3]) -> SessionBuilder<Cfg> {
    let mut builder = builder;
    for (handle, port) in ports.iter().enumerate() {
        let player = if handle == local {
            PlayerType::Local
        } else {
            PlayerType::Remote(localhost(*port))
        };
        builder = builder.add_player(player, handle).unwrap();
    }
    builder
}

fn make_peers(
    ports: [u16; 3],
    rollback_window: Option<usize>,
) -> ([Peer; 3], Arc<Mutex<Option<SocketAddr>>>) {
    let drop_to = Arc::new(Mutex::new(None));

    let a = add_players(builder(rollback_window), 0, ports)
        .start_p2p_session(UdpNonBlockingSocket::bind_to_port(ports[0]).unwrap())
        .unwrap();
    let b = add_players(builder(rollback_window), 1, ports)
        .start_p2p_session(LossySocket {
            inner: UdpNonBlockingSocket::bind_to_port(ports[1]).unwrap(),
            drop_to: drop_to.clone(),
        })
        .unwrap();
    let c = add_players(builder(rollback_window), 2, ports)
        .start_p2p_session(UdpNonBlockingSocket::bind_to_port(ports[2]).unwrap())
        .unwrap();

    let peers = [a, b, c]
        .into_iter()
        .enumerate()
        .map(|(handle, session)| Peer {
            handle,
            session,
            game: Game::default(),
        });
    let peers: Vec<Peer> = peers.collect();
    (peers.try_into().ok().unwrap(), drop_to)
}

fn sync(peers: &mut [Peer; 3]) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        for peer in peers.iter_mut() {
            peer.session.poll_remote_clients();
        }
        if peers
            .iter()
            .all(|peer| peer.session.current_state() == SessionState::Running)
        {
            return;
        }
        thread::sleep(Duration::from_millis(5));
    }
    panic!("peers did not synchronize");
}

fn run_all(peers: &mut [Peer; 3], frames: usize, pause: Duration) {
    for _ in 0..frames {
        for peer in peers.iter_mut() {
            peer.step().unwrap();
        }
        thread::sleep(pause);
    }
}

/// Plays until B is gone from A's and C's point of view, then lets them run on.
/// A has received more of B's inputs than C when B vanishes.
fn play_until_b_dropped(
    ports: [u16; 3],
    rollback_window: Option<usize>,
    extra_frames_c_never_gets: usize,
) -> (Peer, Peer, Result<(), GgrsError>) {
    let (mut peers, drop_to) = make_peers(ports, rollback_window);
    sync(&mut peers);

    run_all(&mut peers, 30, Duration::from_millis(1));

    *drop_to.lock().unwrap() = Some(localhost(ports[2]));
    run_all(
        &mut peers,
        extra_frames_c_never_gets,
        Duration::from_millis(5),
    );

    let [mut a, _b, mut c] = peers;
    let mut outcome = Ok(());
    let deadline = Instant::now() + Duration::from_secs(4);
    let mut settle_frames = 0;
    while Instant::now() < deadline {
        if let Err(e) = a.step() {
            outcome = Err(e);
            break;
        }
        c.step().unwrap();
        if a.game.saw_disconnect && c.game.saw_disconnect {
            settle_frames += 1;
            if settle_frames > 60 {
                break;
            }
        }
        thread::sleep(Duration::from_millis(2));
    }

    assert!(
        a.game.saw_disconnect && c.game.saw_disconnect,
        "disconnect not registered (A: {}, C: {}, A frame {}, C frame {}, outcome {:?})",
        a.game.saw_disconnect,
        c.game.saw_disconnect,
        a.game.frame,
        c.game.frame,
        outcome
    );
    (a, c, outcome)
}

#[test]
#[serial]
fn test_survivors_converge_on_the_smaller_disconnect_frame() {
    let (a, c, outcome) = play_until_b_dropped([7791, 7792, 7793], Some(3 * PREDICTION_WINDOW), 4);
    outcome.expect("recoverable disconnect must not error");

    let last_a = a.game.last_confirmed_dropped_frame();
    let last_c = c.game.last_confirmed_dropped_frame();
    assert!(last_c >= 0);
    assert_eq!(
        last_a, last_c,
        "survivors disagree on the dropped player's last frame"
    );

    let compare_until = a.game.frame.min(c.game.frame) - PREDICTION_WINDOW as Frame;
    for frame in (last_c + 1)..compare_until {
        assert_eq!(
            a.game.dropped_inputs.get(&frame),
            Some(&(0, InputStatus::Disconnected)),
            "A frame {frame}"
        );
        assert_eq!(
            c.game.dropped_inputs.get(&frame),
            Some(&(0, InputStatus::Disconnected)),
            "C frame {frame}"
        );
    }
    for frame in 0..=last_c {
        assert_eq!(
            a.game.dropped_inputs.get(&frame),
            c.game.dropped_inputs.get(&frame),
            "confirmed input differs at frame {frame}"
        );
    }
}

#[test]
#[serial]
fn test_unreachable_disconnect_rollback_is_an_error_not_a_panic() {
    let (mut a, _c, outcome) = play_until_b_dropped([7794, 7795, 7796], None, 4);

    assert!(
        matches!(outcome, Err(GgrsError::RollbackOutOfWindow { .. })),
        "expected RollbackOutOfWindow, got {outcome:?}"
    );
    assert!(
        matches!(a.step(), Err(GgrsError::RollbackOutOfWindow { .. })),
        "the error must persist until the session is reset"
    );
}
