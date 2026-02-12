mod handler;
mod s3;

use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let s3_uri = env::var("S3_INPUT_URI")
        .map_err(|_| "S3_INPUT_URI environment variable not set")?;
    
    let s3_output_uri = env::var("S3_OUTPUT_URI")
        .map_err(|_| "S3_OUTPUT_URI environment variable not set")?;
    
    let openai_api_key = env::var("OPENAI_API_KEY")
        .map_err(|_| "OPENAI_API_KEY environment variable not set")?;

    println!("Processing PDF from: {}", s3_uri);
    println!("Output will be saved to: {}", s3_output_uri);

    handler::process_pdf(&s3_uri, &s3_output_uri).await?;

    println!("PDF processing completed successfully");
    Ok(())
}
