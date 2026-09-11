use std::env;
use thrift::protocol::{
    TBinaryInputProtocol, TBinaryOutputProtocol, TFieldIdentifier, TInputProtocol,
    TMessageIdentifier, TMessageType, TOutputProtocol, TStructIdentifier, TType,
};
use thrift::transport::{TFramedReadTransport, TFramedWriteTransport, TIoChannel, TTcpChannel};

fn main() -> thrift::Result<()> {
    let address = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9090".to_owned());
    let mut channel = TTcpChannel::new();
    channel.open(address)?;
    let (read_channel, write_channel) = channel.split()?;
    let mut input = TBinaryInputProtocol::new(TFramedReadTransport::new(read_channel), true);
    let mut output = TBinaryOutputProtocol::new(TFramedWriteTransport::new(write_channel), true);

    write_echo(&mut output)?;
    let (message, payload) = read_echo(&mut input)?;
    println!("echo message={message} payload_bytes={}", payload.len());

    write_sum(&mut output, &[2, 3, 5, 7])?;
    println!("sum={}", read_i64_result(&mut input, "sum")?);

    write_raise_error(&mut output, 2)?;
    let error_message = input.read_message_begin()?;
    assert_eq!(error_message.message_type, TMessageType::Exception);
    let error = thrift::Error::read_application_error_from_in_protocol(&mut input)?;
    input.read_message_end()?;
    println!("raiseError type={:?} message={}", error.kind, error.message);

    write_notify(&mut output, "hello from biubin")?;
    Ok(())
}

fn write_echo(output: &mut dyn TOutputProtocol) -> thrift::Result<()> {
    output.write_message_begin(&TMessageIdentifier::new("echo", TMessageType::Call, 1))?;
    output.write_struct_begin(&TStructIdentifier::new("echo_args"))?;
    output.write_field_begin(&TFieldIdentifier::new("request", TType::Struct, 1))?;
    output.write_struct_begin(&TStructIdentifier::new("EchoRequest"))?;
    output.write_field_begin(&TFieldIdentifier::new("message", TType::String, 1))?;
    output.write_string("hello from biubin")?;
    output.write_field_end()?;
    output.write_field_begin(&TFieldIdentifier::new("payload", TType::String, 2))?;
    output.write_bytes(b"payload")?;
    output.write_field_end()?;
    output.write_field_stop()?;
    output.write_struct_end()?;
    output.write_field_end()?;
    output.write_field_stop()?;
    output.write_struct_end()?;
    output.write_message_end()?;
    output.flush()
}

fn read_echo(input: &mut dyn TInputProtocol) -> thrift::Result<(String, Vec<u8>)> {
    let message = input.read_message_begin()?;
    assert_eq!(message.message_type, TMessageType::Reply);
    input.read_struct_begin()?;
    let mut result = (String::new(), Vec::new());
    loop {
        let field = input.read_field_begin()?;
        if field.field_type == TType::Stop {
            break;
        }
        if field.id == Some(0) && field.field_type == TType::Struct {
            input.read_struct_begin()?;
            loop {
                let nested = input.read_field_begin()?;
                if nested.field_type == TType::Stop {
                    break;
                }
                match (nested.id, nested.field_type) {
                    (Some(1), TType::String) => result.0 = input.read_string()?,
                    (Some(2), TType::String) => result.1 = input.read_bytes()?,
                    (_, field_type) => input.skip(field_type)?,
                }
                input.read_field_end()?;
            }
            input.read_struct_end()?;
        } else {
            input.skip(field.field_type)?;
        }
        input.read_field_end()?;
    }
    input.read_struct_end()?;
    input.read_message_end()?;
    Ok(result)
}

fn write_sum(output: &mut dyn TOutputProtocol, values: &[i64]) -> thrift::Result<()> {
    output.write_message_begin(&TMessageIdentifier::new("sum", TMessageType::Call, 2))?;
    output.write_struct_begin(&TStructIdentifier::new("sum_args"))?;
    output.write_field_begin(&TFieldIdentifier::new("values", TType::List, 1))?;
    output.write_list_begin(&thrift::protocol::TListIdentifier::new(
        TType::I64,
        values.len() as i32,
    ))?;
    for value in values {
        output.write_i64(*value)?;
    }
    output.write_list_end()?;
    output.write_field_end()?;
    output.write_field_stop()?;
    output.write_struct_end()?;
    output.write_message_end()?;
    output.flush()
}

fn read_i64_result(input: &mut dyn TInputProtocol, name: &str) -> thrift::Result<i64> {
    let message = input.read_message_begin()?;
    assert_eq!(message.name, name);
    assert_eq!(message.message_type, TMessageType::Reply);
    input.read_struct_begin()?;
    let mut result = 0;
    loop {
        let field = input.read_field_begin()?;
        if field.field_type == TType::Stop {
            break;
        }
        if field.id == Some(0) && field.field_type == TType::I64 {
            result = input.read_i64()?;
        } else {
            input.skip(field.field_type)?;
        }
        input.read_field_end()?;
    }
    input.read_struct_end()?;
    input.read_message_end()?;
    Ok(result)
}

fn write_raise_error(output: &mut dyn TOutputProtocol, kind: i32) -> thrift::Result<()> {
    output.write_message_begin(&TMessageIdentifier::new(
        "raiseError",
        TMessageType::Call,
        3,
    ))?;
    output.write_struct_begin(&TStructIdentifier::new("raise_error_args"))?;
    output.write_field_begin(&TFieldIdentifier::new("kind", TType::I32, 1))?;
    output.write_i32(kind)?;
    output.write_field_end()?;
    output.write_field_stop()?;
    output.write_struct_end()?;
    output.write_message_end()?;
    output.flush()
}

fn write_notify(output: &mut dyn TOutputProtocol, message: &str) -> thrift::Result<()> {
    output.write_message_begin(&TMessageIdentifier::new("notify", TMessageType::OneWay, 4))?;
    output.write_struct_begin(&TStructIdentifier::new("notify_args"))?;
    output.write_field_begin(&TFieldIdentifier::new("message", TType::String, 1))?;
    output.write_string(message)?;
    output.write_field_end()?;
    output.write_field_stop()?;
    output.write_struct_end()?;
    output.write_message_end()?;
    output.flush()
}
