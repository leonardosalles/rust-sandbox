use anyhow::{Context, Result};
use quick_xml::events::Event;
use quick_xml::Reader;
use tracing::{info, instrument, warn};

use super::sqlite_source::SaleRecord;

#[instrument(name = "ingest_soap", skip_all)]
pub async fn ingest_from_soap(soap_url: &str) -> Result<(Vec<SaleRecord>, u64)> {
    let client = reqwest::Client::new();

    let soap_body = r#"<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
               xmlns:sale="http://kata-data.example.com/sales/v1">
    <soap:Header/>
    <soap:Body>
        <sale:GetSalesDataRequest>
            <sale:RequestId>pipeline-ingestion-001</sale:RequestId>
            <sale:PageSize>100</sale:PageSize>
        </sale:GetSalesDataRequest>
    </soap:Body>
</soap:Envelope>"#;

    let response = client
        .post(soap_url)
        .header("Content-Type", "text/xml; charset=utf-8")
        .header("SOAPAction", "\"GetSalesData\"")
        .body(soap_body)
        .send()
        .await
        .context("Failed to call SOAP service")?;

    let xml_text = response.text().await?;
    info!(bytes = %xml_text.len(), "Received SOAP response");

    let records = parse_soap_sales_response(&xml_text)?;
    let count = records.len() as u64;
    info!(source = "soap_ws", records = %count, url = %soap_url, "Ingested records from SOAP WS");
    Ok((records, count))
}

fn parse_soap_sales_response(xml: &str) -> Result<Vec<SaleRecord>> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut records = Vec::new();
    let mut current: Option<PartialRecord> = None;
    let mut current_tag = String::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let tag = String::from_utf8_lossy(e.local_name().as_ref()).to_string();
                current_tag = tag.clone();
                if tag == "SaleRecord" {
                    current = Some(PartialRecord::default());
                }
            }
            Ok(Event::Text(e)) => {
                let text = e.unescape().unwrap_or_default().to_string();
                if let Some(ref mut rec) = current {
                    match current_tag.as_str() {
                        "SaleId" => rec.sale_id = text,
                        "City" => rec.city = text,
                        "State" => rec.state = text,
                        "Amount" => rec.amount = text.parse().unwrap_or(0.0),
                        "Quantity" => rec.quantity = text.parse().unwrap_or(0),
                        "SalesmanId" => rec.salesman_id = text,
                        "Timestamp" => rec.timestamp = text,
                        _ => {}
                    }
                }
            }
            Ok(Event::End(e)) => {
                let tag = String::from_utf8_lossy(e.local_name().as_ref()).to_string();
                if tag == "SaleRecord" {
                    if let Some(rec) = current.take() {
                        records.push(SaleRecord {
                            sale_id: rec.sale_id,
                            city: rec.city,
                            state: rec.state,
                            salesman_name: format!("SOAP_{}", rec.salesman_id),
                            salesman_id: rec.salesman_id,
                            amount: rec.amount,
                            quantity: rec.quantity,
                            timestamp: rec.timestamp,
                            source: "soap_ws".into(),
                        });
                    }
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                warn!("XML parse error: {}", e);
                break;
            }
            _ => {}
        }
    }

    Ok(records)
}

#[derive(Default)]
struct PartialRecord {
    sale_id: String,
    city: String,
    state: String,
    salesman_id: String,
    amount: f64,
    quantity: i64,
    timestamp: String,
}
