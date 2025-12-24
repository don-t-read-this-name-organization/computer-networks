use voip_backend::{jitter::JitterBuffer, packet::AudioPacket};

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

    let jitter = Arc::new(Mutex::new(JitterBuffer::new()));
    let jitter_clone = jitter.clone();
    spawn(move || {
        let mut buf = [0u8; 4096];
        let mut last_seq: Option<u16> = None;
        loop {
            if let Ok((size, _)) = socket.recv_from(&mut buf) {
                if let Some(packet) = AudioPacket::deserialize(&buf[..size]) {
                    if let Some(prev) = last_seq {
                        let exprected = prev.wrapping_add(1);
                        if packet.seq != exprected {
                            println!("Packet loss: exprected {}, got {}", exprected, packet.seq);
                        }
                    }

                    last_seq = Some(packet.seq);

                    let mut jb = jitter_clone.lock().unwrap();
                    jb.push_packet(&packet.samples);
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
                let mut jb = jitter.lock().unwrap();
                for s in data {
                    let sample = jb.pop_sample();
                    *s = sample as f32 / i16::MAX as f32;
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
