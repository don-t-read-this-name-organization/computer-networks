use std::error::Error;
use std::sync::atomic::{AtomicU16, Ordering::Relaxed};
use std::sync::{Arc, Mutex};

use crate::jitter::JitterBuffer;
use crate::packet::AudioPacket;
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{Host, Stream, StreamError};
use tokio::sync::mpsc::Sender;

pub fn input_stream_fn(host: Host, channel: Sender<Vec<u8>>) -> Result<Stream, Box<dyn Error>> {
    let input_device = host.default_input_device().expect("No input device");
    let input_config = input_device.default_input_config()?;

    println!("Input config: {:?}", input_config);

    let seq = AtomicU16::new(0);

    let stream = match input_config.sample_format() {
        cpal::SampleFormat::F32 => input_device.build_input_stream(
            &input_config.into(),
            move |data: &[f32], _| {
                let mut samples = Vec::with_capacity(data.len());
                for &s in data {
                    samples.push((s * i16::MAX as f32) as i16);
                }
                let packet = AudioPacket {
                    seq: seq.fetch_add(1, Relaxed),
                    samples,
                };

                let _ = channel.try_send(packet.serialize());
            },
            err_fn,
            None,
        )?,
        _ => panic!("Unsupported format"),
    };

    stream.play()?;
    println!("Sending audio...");
    Ok(stream)
}

pub fn output_stream_fn(
    host: Host,
    buffer: Arc<Mutex<JitterBuffer>>,
) -> Result<Stream, Box<dyn Error>> {
    let output_device = host.default_output_device().expect("No output device");
    let output_config = output_device.default_output_config()?;

    println!("Output config: {:?}", output_config);

    let stream = match output_config.sample_format() {
        cpal::SampleFormat::F32 => output_device.build_output_stream(
            &output_config.into(),
            move |data: &mut [f32], _| {
                let mut jb = buffer.lock().unwrap();
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
    Ok(stream)
}

fn err_fn(err: StreamError) {
    eprintln!("Stream error: {}", err);
}
