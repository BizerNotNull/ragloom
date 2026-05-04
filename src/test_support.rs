use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub(crate) struct TestHttpResponse {
    status: u16,
    reason: &'static str,
    body: &'static str,
}

impl TestHttpResponse {
    pub(crate) fn json(status: u16, body: &'static str) -> Self {
        let reason = match status {
            200 => "OK",
            404 => "Not Found",
            409 => "Conflict",
            500 => "Internal Server Error",
            _ => "Test Response",
        };

        Self {
            status,
            reason,
            body,
        }
    }
}

pub(crate) struct ScriptedHttpServer {
    base_url: String,
    requests: Arc<Mutex<Vec<String>>>,
    handle: std::thread::JoinHandle<()>,
}

impl ScriptedHttpServer {
    pub(crate) fn base_url(&self) -> String {
        self.base_url.clone()
    }

    pub(crate) fn join(self) -> Vec<String> {
        self.handle.join().expect("join scripted server");
        self.requests.lock().expect("requests lock").clone()
    }
}

pub(crate) fn spawn_scripted_http_server(responses: Vec<TestHttpResponse>) -> ScriptedHttpServer {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    let responses = Arc::new(Mutex::new(VecDeque::from(responses)));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let thread_responses = Arc::clone(&responses);
    let thread_requests = Arc::clone(&requests);

    let handle = std::thread::spawn(move || {
        loop {
            let response = {
                let mut guard = thread_responses.lock().expect("responses lock");
                guard.pop_front()
            };

            let Some(response) = response else {
                break;
            };

            let (mut stream, _) = listener.accept().expect("accept");
            let request = read_http_request(&mut stream);
            thread_requests
                .lock()
                .expect("requests lock")
                .push(String::from_utf8_lossy(&request).into_owned());

            write!(
                stream,
                "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response.status,
                response.reason,
                response.body.len(),
                response.body
            )
            .expect("write response");
            let _ = stream.flush();
            let _ = stream.shutdown(Shutdown::Both);
        }
    });

    ScriptedHttpServer {
        base_url: format!("http://{addr}"),
        requests,
        handle,
    }
}

fn read_http_request(stream: &mut impl Read) -> Vec<u8> {
    let mut buf = [0_u8; 8192];
    let mut request = Vec::new();

    loop {
        let read = stream.read(&mut buf).expect("read request");
        if read == 0 {
            return request;
        }

        request.extend_from_slice(&buf[..read]);

        let Some(header_end) = request.windows(4).position(|w| w == b"\r\n\r\n") else {
            continue;
        };
        let header_end = header_end + 4;
        let content_length = content_length(&request[..header_end]);

        while request.len() < header_end + content_length {
            let read = stream.read(&mut buf).expect("read body");
            if read == 0 {
                break;
            }
            request.extend_from_slice(&buf[..read]);
        }

        return request;
    }
}

fn content_length(headers: &[u8]) -> usize {
    String::from_utf8_lossy(headers)
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            if name.eq_ignore_ascii_case("content-length") {
                value.trim().parse().ok()
            } else {
                None
            }
        })
        .unwrap_or(0)
}
