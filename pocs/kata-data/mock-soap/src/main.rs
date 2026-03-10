use axum::{
    body::Body,

    http::{header, StatusCode},
    response::Response,
    routing::post,
    Router,
};
use chrono::Utc;
use rand::Rng;
use tracing::info;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("mock_soap=debug,info")
        .init();

    let app = Router::new()
        .route("/soap/sales", post(handle_soap_request))
        .route("/wsdl", axum::routing::get(serve_wsdl));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8081").await.unwrap();
    info!("Mock SOAP server running on http://0.0.0.0:8081");
    info!("WSDL available at http://localhost:8081/wsdl");
    axum::serve(listener, app).await.unwrap();
}

async fn handle_soap_request(body: String) -> Response<Body> {
    info!("Received SOAP request: {} bytes", body.len());

    let action = if body.contains("GetSalesData") {
        "GetSalesData"
    } else if body.contains("GetSalesmanData") {
        "GetSalesmanData"
    } else {
        "GetSalesData"
    };

    let response_xml = match action {
        "GetSalesmanData" => generate_salesman_soap_response(),
        _ => generate_sales_soap_response(),
    };

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/xml; charset=utf-8")
        .header("SOAPAction", format!("\"{}Response\"", action))
        .body(Body::from(response_xml))
        .unwrap()
}

fn generate_sales_soap_response() -> String {
    let mut rng = rand::thread_rng();
    let timestamp = Utc::now().format("%Y-%m-%dT%H:%M:%SZ");

    let cities = [
        ("São Paulo", "SP"),
        ("Rio de Janeiro", "RJ"),
        ("Belo Horizonte", "MG"),
        ("Curitiba", "PR"),
        ("Porto Alegre", "RS"),
        ("Salvador", "BA"),
        ("Recife", "PE"),
        ("Fortaleza", "CE"),
    ];

    let mut sales_items = String::new();
    for (city, state) in &cities {
        let amount: f64 = rng.gen_range(5000.0..150000.0);
        let quantity: u32 = rng.gen_range(10..500);
        sales_items.push_str(&format!(
            r#"
            <sale:SaleRecord>
                <sale:SaleId>{}</sale:SaleId>
                <sale:City>{}</sale:City>
                <sale:State>{}</sale:State>
                <sale:Amount>{:.2}</sale:Amount>
                <sale:Quantity>{}</sale:Quantity>
                <sale:SalesmanId>SM{:04}</sale:SalesmanId>
                <sale:Timestamp>{}</sale:Timestamp>
                <sale:Source>SOAP_WS</sale:Source>
            </sale:SaleRecord>"#,
            uuid_simple(),
            city,
            state,
            amount,
            quantity,
            rng.gen_range(1..50),
            timestamp
        ));
    }

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope
    xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
    xmlns:sale="http://kata-data.example.com/sales/v1">
    <soap:Header>
        <sale:ResponseHeader>
            <sale:RequestId>{}</sale:RequestId>
            <sale:Timestamp>{}</sale:Timestamp>
            <sale:ServiceVersion>1.0</sale:ServiceVersion>
        </sale:ResponseHeader>
    </soap:Header>
    <soap:Body>
        <sale:GetSalesDataResponse>
            <sale:Status>SUCCESS</sale:Status>
            <sale:SalesData>
                {}
            </sale:SalesData>
        </sale:GetSalesDataResponse>
    </soap:Body>
</soap:Envelope>"#,
        uuid_simple(),
        timestamp,
        sales_items
    )
}

fn generate_salesman_soap_response() -> String {
    let mut rng = rand::thread_rng();
    let timestamp = Utc::now().format("%Y-%m-%dT%H:%M:%SZ");

    let salesmen = [
        ("João Silva", "SP"),
        ("Maria Santos", "RJ"),
        ("Carlos Oliveira", "MG"),
        ("Ana Lima", "PR"),
        ("Pedro Souza", "RS"),
        ("Lucia Costa", "BA"),
        ("Roberto Ferreira", "PE"),
        ("Fernanda Alves", "CE"),
    ];

    let mut salesman_items = String::new();
    for (i, (name, state)) in salesmen.iter().enumerate() {
        let total_sales: f64 = rng.gen_range(50000.0..500000.0);
        salesman_items.push_str(&format!(
            r#"
            <sale:SalesmanRecord>
                <sale:SalesmanId>SM{:04}</sale:SalesmanId>
                <sale:Name>{}</sale:Name>
                <sale:State>{}</sale:State>
                <sale:TotalSales>{:.2}</sale:TotalSales>
                <sale:TransactionCount>{}</sale:TransactionCount>
                <sale:Source>SOAP_WS</sale:Source>
            </sale:SalesmanRecord>"#,
            i + 1,
            name,
            state,
            total_sales,
            rng.gen_range(50..300)
        ));
    }

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope
    xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
    xmlns:sale="http://kata-data.example.com/sales/v1">
    <soap:Header>
        <sale:ResponseHeader>
            <sale:RequestId>{}</sale:RequestId>
            <sale:Timestamp>{}</sale:Timestamp>
        </sale:ResponseHeader>
    </soap:Header>
    <soap:Body>
        <sale:GetSalesmanDataResponse>
            <sale:Status>SUCCESS</sale:Status>
            <sale:SalesmanData>
                {}
            </sale:SalesmanData>
        </sale:GetSalesmanDataResponse>
    </soap:Body>
</soap:Envelope>"#,
        uuid_simple(),
        timestamp,
        salesman_items
    )
}

async fn serve_wsdl() -> Response<Body> {
    let wsdl = r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions name="SalesService"
    targetNamespace="http://kata-data.example.com/sales/v1"
    xmlns="http://schemas.xmlsoap.org/wsdl/"
    xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
    xmlns:tns="http://kata-data.example.com/sales/v1"
    xmlns:xsd="http://www.w3.org/2001/XMLSchema">

    <message name="GetSalesDataRequest">
        <part name="parameters" element="tns:GetSalesDataRequest"/>
    </message>
    <message name="GetSalesDataResponse">
        <part name="parameters" element="tns:GetSalesDataResponse"/>
    </message>

    <portType name="SalesPortType">
        <operation name="GetSalesData">
            <input message="tns:GetSalesDataRequest"/>
            <output message="tns:GetSalesDataResponse"/>
        </operation>
    </portType>

    <binding name="SalesBinding" type="tns:SalesPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
        <operation name="GetSalesData">
            <soap:operation soapAction="GetSalesData"/>
        </operation>
    </binding>

    <service name="SalesService">
        <port name="SalesPort" binding="tns:SalesBinding">
            <soap:address location="http://localhost:8081/soap/sales"/>
        </port>
    </service>
</definitions>"#;

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/xml; charset=utf-8")
        .body(Body::from(wsdl))
        .unwrap()
}

fn uuid_simple() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    format!("{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}", t, t >> 16, t & 0xfff, t >> 8 & 0x3fff | 0x8000, t as u64 * 1000003)
}
