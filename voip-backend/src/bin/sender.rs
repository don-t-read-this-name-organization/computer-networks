use std::{f32, net::UdpSocket, thread::park};

use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let socket = UdpSocket::bind("0.0.0.0:0")?;
    // The IP of the counterpart (should be adjusted)
    socket.connect("192.168.4.1:40000")?;

    let host = cpal::default_host();
    let device = host.default_input_device().expect("No input device");
    let config = device.default_input_config()?;

    println!("Input config: {:?}", config);

    let stream = match config.sample_format() {
        cpal::SampleFormat::F32 => device.build_input_stream(
            &config.into(),
            move |data: &[f32], _| {
                let mut buffer = Vec::with_capacity(data.len() * 2);
                for &sample in data {
                    let s = (sample * i16::MAX as f32) as i16;
                    buffer.extend_from_slice(&s.to_le_bytes());
                }
                let _ = socket.send(&buffer);
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
