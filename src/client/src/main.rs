use std::net::TcpStream;
use std::io::Read;
use std::io::Write;

fn main() -> std::io::Result<()> {
    let mut stream = TcpStream::connect("127.0.0.1:8080")?;

    //try adding a topic, then subscribe to it
    
    
    stream.write_all(b"Hello from client!")?;
    
    let mut buffer = [0; 1024];
    let size = stream.read(&mut buffer)?;
    println!("Received: {}", String::from_utf8_lossy(&buffer[..size]));

    Ok(())
}
