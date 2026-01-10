use std::sync::atomic::{AtomicU16, Ordering::Relaxed};
use std::sync::{Arc, Mutex};

use crate::jitter::JitterBuffer;
use crate::packet::AudioPacket;
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{Device, Host, Stream, StreamError};
use tokio::sync::broadcast::Sender;

pub struct AudioState {
    host: Host,
    input: Option<Stream>,
    output: Option<Stream>,
}

impl AudioState {
    pub fn new(host: Host) -> Self {
        Self {
            host,
            input: None,
            output: None,
        }
    }

    pub fn start(
        &mut self,
        input_channel: Sender<Vec<u8>>,
        output_jitter: Arc<Mutex<JitterBuffer>>,
    ) {
        let output_device = self.host.default_output_device().expect("No output device");
        self.output = output_stream_fn(output_device, output_jitter).unwrap_or(None);
        if let Some(input_device) = self.host.default_input_device() {
            self.input = input_stream_fn(input_device, input_channel).unwrap_or(None);
        } else {
            self.input = None;
        }
        if self.output.is_none() {
            self.clear();
            eprintln!("Failed to create output stream");
        }
    }

    pub fn clear(&mut self) {
        self.input = None;
        self.output = None;
    }
}

pub fn input_stream_fn(
    input_device: Device,
    channel: Sender<Vec<u8>>,
) -> Result<Option<Stream>, ()> {
    let input_config = match input_device.default_input_config() {
        Ok(config) => config,
        Err(e) => {
            eprintln!("input config error: {}", e);
            return Err(());
        }
    };

    let stream_config = cpal::StreamConfig {
        channels: input_config.channels(),
        sample_rate: 44100,
        buffer_size: cpal::BufferSize::Fixed(256),
    };

    println!("Input config: {:?}", stream_config);

    let seq = AtomicU16::new(0);

    let stream = match input_config.sample_format() {
        cpal::SampleFormat::F32 => input_device.build_input_stream(
            &stream_config,
            move |data: &[f32], _| {
                let mut samples = Vec::with_capacity(data.len());
                for &s in data {
                    samples.push((s * i16::MAX as f32) as i16);
                }
                let packet = AudioPacket {
                    seq: seq.fetch_add(1, Relaxed),
                    samples,
                };

                let _ = channel.send(packet.serialize());
            },
            err_fn,
            None,
        ),
        _ => panic!("Unsupported format"),
    };

    match stream {
        Ok(s) => {
            s.play().unwrap_or_default();
            println!("Sending audio...");
            Ok(Some(s))
        }
        Err(e) => {
            eprintln!("Input stream error: {}", e);
            Err(())
        }
    }
}

pub fn output_stream_fn(
    output_device: Device,
    buffer: Arc<Mutex<JitterBuffer>>,
) -> Result<Option<Stream>, ()> {
    let output_config = match output_device.default_output_config() {
        Ok(config) => config,
        Err(e) => {
            eprintln!("output config error: {}", e);
            return Err(());
        }
    };

    let stream_config = cpal::StreamConfig {
        channels: output_config.channels(),
        sample_rate: 44100,
        buffer_size: cpal::BufferSize::Fixed(256),
    };

    println!("Output config: {:?}", stream_config);

    let stream = match output_config.sample_format() {
        cpal::SampleFormat::F32 => output_device.build_output_stream(
            &stream_config,
            move |data: &mut [f32], _| {
                let mut jb = buffer.lock().unwrap();
                for s in data {
                    let sample = jb.pop_sample();
                    *s = sample as f32 / i16::MAX as f32;
                }
            },
            err_fn,
            None,
        ),
        _ => panic!("Unsupported format"),
    };

    match stream {
        Ok(s) => {
            s.play().unwrap_or_default();
            println!("Receiving audio...");
            Ok(Some(s))
        }
        Err(e) => {
            eprintln!("Output stream error: {}", e);
            Err(())
        }
    }
}

fn err_fn(err: StreamError) {
    eprintln!("Stream error: {}", err);
}
