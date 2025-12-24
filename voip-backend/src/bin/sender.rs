use std::{
    f32,
    net::UdpSocket,
    sync::{Arc, Mutex},
    thread::park,
};
use voip_backend::packet::AudioPacket;

use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let socket = UdpSocket::bind("0.0.0.0:0")?;
    // The IP of the counterpart (should be adjusted)
    socket.connect("192.168.4.1:40000")?;

    let seq = Arc::new(Mutex::new(0u16));

    let host = cpal::default_host();
    let device = host.default_input_device().expect("No input device");
    let config = device.default_input_config()?;

    println!("Input config: {:?}", config);

    let seq_clone = seq.clone();

    let stream = match config.sample_format() {
        cpal::SampleFormat::F32 => device.build_input_stream(
            &config.into(),
            move |data: &[f32], _| {
                let mut samples = Vec::with_capacity(data.len());
                for &s in data {
                    samples.push((s * i16::MAX as f32) as i16);
                }
                let mut guard = seq_clone.lock().unwrap();
                let packet = AudioPacket {
                    seq: *guard,
                    samples,
                };
                *guard = guard.wrapping_add(1);

                let _ = socket.send(&packet.serialize());
            },
            err_fn,
            None,
        )?,
        _ => panic!("Unsupported format"),
    };

    stream.play()?;
    println!("Sending audio...");

    park();
    Ok(())
}

fn err_fn(err: cpal::StreamError) {
    eprintln!("Stream error: {}", err);
}
