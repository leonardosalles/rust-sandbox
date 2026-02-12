use serde_json::json;
use std::fs;
use pdf_extract::extract_text;
use reqwest::Client;

use crate::s3::{download_s3_file, upload_s3_file};

pub async fn process_pdf(s3_uri: &str, s3_output_uri: &str, openai_api_key: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("Downloading PDF from: {}", s3_uri);
    
    let pdf_bytes = download_s3_file(s3_uri).await?;
    println!("Downloaded {} bytes", pdf_bytes.len());

    let extracted_text = extract_text_from_pdf(&pdf_bytes)?;
    println!("Extracted text length: {} characters", extracted_text.len());

    let summary = summarize_text(&extracted_text, openai_api_key).await?;
    println!("Generated summary length: {} characters", summary.len());

    let task_def_content = fs::read_to_string("task.definition.json")?;
    let task_def: serde_json::Value = serde_json::from_str(&task_def_content)?;
    
    let output_json = create_output_from_schema(&task_def["outputSchema"], &summary)?;
    
    upload_s3_file(s3_output_uri, output_json.to_string().as_bytes()).await?;
    
    println!("Uploaded summary to: {}", s3_output_uri);
    
    Ok(())
}

fn extract_text_from_pdf(pdf_bytes: &[u8]) -> Result<String, Box<dyn std::error::Error>> {
    println!("Extracting text from PDF...");
    
    let text = extract_text(std::io::Cursor::new(pdf_bytes))?;
    
    if text.trim().is_empty() {
        return Err("No text could be extracted from PDF".into());
    }
    
    println!("Successfully extracted {} characters from PDF", text.len());
    Ok(text)
}

async fn summarize_text(text: &str, openai_api_key: &str) -> Result<String, Box<dyn std::error::Error>> {
    println!("Summarizing extracted text using OpenAI...");
    
    let client = Client::new();
    
    let max_chars = 8000;
    let truncated_text = if text.len() > max_chars {
        &text[..max_chars]
    } else {
        text
    };
    
    let request_body = json!({
        "model": "gpt-3.5-turbo",
        "messages": [
            {
                "role": "system",
                "content": "You are a helpful assistant that summarizes documents. Create a concise summary of the main points and key information from the provided text."
            },
            {
                "role": "user",
                "content": format!("Please summarize the following text:\n\n{}", truncated_text)
            }
        ],
        "max_tokens": 500,
        "temperature": 0.3
    });
    
    let response = client
        .post("https://api.openai.com/v1/chat/completions")
        .header("Authorization", format!("Bearer {}", openai_api_key))
        .header("Content-Type", "application/json")
        .json(&request_body)
        .send()
        .await?;
    
    if !response.status().is_success() {
        let error_text = response.text().await?;
        return Err(format!("OpenAI API request failed: {}", error_text).into());
    }
    
    let response_json: serde_json::Value = response.json().await?;
    
    let summary = response_json["choices"][0]["message"]["content"]
        .as_str()
        .ok_or("Invalid response format from OpenAI API")?;
    
    println!("Successfully generated summary using OpenAI");
    Ok(summary.trim().to_string())
}

fn create_output_from_schema(output_schema: &serde_json::Value, summary: &str) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let mut output = serde_json::Map::new();
    
    if let Some(fields) = output_schema["fields"].as_array() {
        for field in fields {
            if let Some(field_name) = field["field"].as_str() {
                if field_name == "summary" {
                    output.insert(field_name.to_string(), serde_json::Value::String(summary.to_string()));
                }
            }
        }
    }
    
    Ok(serde_json::Value::Object(output))
}
