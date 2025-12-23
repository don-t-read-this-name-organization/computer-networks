use std::{
    net::UdpSocket,
    sync::{Arc, Mutex},
    thread::{park, spawn},
};

use cpal::{
    default_host,
    traits::{DeviceTrait, HostTrait, StreamTrait},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let socket = UdpSocket::bind("0.0.0.0:40000")?;
    println!("Listening on UDP 40000");

    let buffer: Arc<Mutex<Vec<i16>>> = Arc::new(Mutex::new(Vec::new()));
    let buffer_clone = buffer.clone();

    spawn(move || {
        let mut recv_buf = [0u8; 4096];
        loop {
            if let Ok((size, _)) = socket.recv_from(&mut recv_buf) {
                let mut guard = buffer_clone.lock().unwrap();
                for chunk in recv_buf[..size].chunks_exact(2) {
                    let sample = i16::from_le_bytes([chunk[0], chunk[1]]);
                    guard.push(sample);
                }
            }
        }
    });

    let host = default_host();
    let device = host.default_output_device().expect("No output device");
    let config = device.default_output_config()?;

    println!("Output config: {:?}", config);

    let stream = match config.sample_format() {
        cpal::SampleFormat::F32 => device.build_output_stream(
            &config.into(),
            move |data: &mut [f32], _| {
                let mut guard = buffer.lock().unwrap();
                for sample in data {
                    if !guard.is_empty() {
                        let s = guard.remove(0);
                        *sample = s as f32 / i16::MAX as f32;
                    } else {
                        *sample = 0.0;
                    }
                }
            },
            err_fn,
            None,
        )?,
        _ => panic!("Unsupported format"),
    };

    stream.play()?;
    println!("Receiving audio...");
    park();
    Ok(())
}

fn err_fn(err: cpal::StreamError) {
    eprintln!("Stream error: {}", err);
}
