use std::fs::File;
use std::io::Write;
use std::io;
use reqwest;
use scraper::{Html, Selector};
use serde::{Serialize, Deserialize};
use serde_json;
use lettre::{Message, SmtpTransport, Transport};
use lettre::transport::smtp::authentication::Credentials;
use askama::Template;
use redis::Commands;
use chrono::Utc;
use std::thread;
use std::time::Duration;
use thiserror::Error;
use lettre::error::Error as LettreError;
use redis::RedisError;
use askama::Error as AskamaError;

#[derive(Error, Debug)]
pub enum MyError {
    #[error("Network error: {0}")]
    NetworkError(#[from] reqwest::Error),

    #[error("Data parsing error: {0}")]
    DataParseError(#[from] serde_json::Error),

    #[error("I/O error: {0}")]
    IOError(#[from] io::Error),

    #[error("Email sending error: {0}")]
    EmailError(#[from] LettreError),

    #[error("SMTP transport error: {0}")]
    SmtpTransportError(#[from] lettre::transport::smtp::Error),

    #[error("Redis operation error: {0}")]
    RedisError(#[from] RedisError),
    
    #[error("Template rendering error: {0}")]
    TemplateError(#[from] AskamaError),
    
    #[error("Invalid data: {0}")]
    InvalidData(String),
}

#[derive(Serialize, Deserialize, Debug)]
struct Business {
    url: String,
    email: String,
}

#[derive(Template)]
#[template(path = "email_template.html")]
struct EmailTemplate {
    subject: String,
    name: String,
    position: String,
    contact_information: String,
    website_url: String,
}

fn current_day() -> String {
    Utc::now().format("%Y-%m-%d").to_string()
}

fn check_and_update_email_count(con: &mut redis::Connection, max_emails_per_day: usize) -> Result<bool, MyError> {
    let today = current_day();
    let key = format!("emails_sent:{}", today);
    let mut retry_count = 0;
    let max_retries = 5;

    loop {
        match con.get(&key) {
            Ok(count) => {
                let count: isize = count;
                if count < max_emails_per_day as isize {
                    let _: () = con.incr(&key, 1).map_err(MyError::RedisError)?;
                    if count == 0 {
                        let _: () = con.expire(&key, 86400).map_err(MyError::RedisError)?;
                    }
                    return Ok(true);
                } else {
                    return Ok(false);
                }
            },
            Err(err) => {
                if retry_count >= max_retries {
                    return Err(MyError::RedisError(err));
                } else {
                    eprintln!("Redis failed, retrying... (Attempt: {})", retry_count + 1);
                    retry_count += 1;
                    thread::sleep(Duration::from_millis(500)); // Wait for 500 ms
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), MyError> {
    let client = reqwest::Client::new();
    let mut page_number = 1;
    let mut has_more_pages = true;
    let mut businesses: Vec<Business> = Vec::new();

    // Establish Redis connection
    let redis_client = redis::Client::open("redis://127.0.0.1/").map_err(MyError::RedisError)?;
    let mut redis_con = redis_client.get_connection().map_err(MyError::RedisError)?;

    let max_emails_per_day = 400;

    while has_more_pages {
        let list_page_url = format!(
            "https://www.yellowpages.com/search?search_terms=event+planning&geo_location_terms=Columbus%2C+OH&page={}",
            page_number
        );

        let list_page_response = client.get(&list_page_url)
            .send()
            .await
            .map_err(MyError::NetworkError)?
            .text()
            .await
            .map_err(MyError::NetworkError)?;
        
        let list_page_document = Html::parse_document(&list_page_response);
        let business_link_selector = Selector::parse("a.business-name").unwrap();
        let business_links: Vec<_> = list_page_document.select(&business_link_selector).collect();

        if business_links.is_empty() {
            // has_more_pages = false;
            break;
        }

        has_more_pages = !business_links.is_empty();

        if has_more_pages {
            page_number += 1;
        }

        for link_element in business_links {
            if let Some(href) = link_element.value().attr("href") {
                let detail_url = format!("https://www.yellowpages.com{}", href);
                let detail_page_response = client.get(&detail_url)
                    .send()
                    .await
                    .map_err(MyError::NetworkError)?
                    .text()
                    .await
                    .map_err(MyError::NetworkError)?;
                
                let detail_page_document = Html::parse_document(&detail_page_response);
                let email_selector = Selector::parse("a.email-business").unwrap();

                if let Some(email_element) = detail_page_document.select(&email_selector).next() {
                    let email = email_element.inner_html();
                    println!("Business URL: {}", detail_url);
                    println!("Business Email: {}", email);

                    businesses.push(Business {
                        url: detail_url,
                        email: email,
                    });
                }

                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }

        page_number += 1;
    }

    let json_data = serde_json::to_string_pretty(&businesses).map_err(MyError::DataParseError)?;
    let mut file = File::create("business_emails.json").map_err(MyError::IOError)?;
    file.write_all(json_data.as_bytes()).map_err(MyError::IOError)?;

    let email_sender = "coffeecodestudio.dev@gmail.com";
    let creds = Credentials::new(email_sender.to_string(), "<add password>".to_string());
    let mailer = SmtpTransport::relay("smtp.gmail.com")?.credentials(creds).build();
    
    let subject = "Grow Your Business with Coffee Code Studio - Special Offer Inside!".to_string();
    let name = "Tim Reed".to_string();
    let position = "Lead Developer".to_string();
    let contact_information = "coffeecodestudio.dev@gmail.com".to_string();
    let website_url = "https://coffeecodestudio.com/".to_string();

    let email_template = EmailTemplate {
        subject: subject.clone(),
        name,
        position,
        contact_information,
        website_url,
    };

    let email_content = email_template.render().map_err(MyError::TemplateError)?;

    for business in &businesses {
        if !business.email.contains('@') {
            println!("Invalid email skipped: {}", business.email);
            continue;
        }

        if check_and_update_email_count(&mut redis_con, max_emails_per_day)? {
            let email = Message::builder()
                .from(email_sender.parse().unwrap())
                .to(business.email.parse().unwrap())
                .subject(&subject)
                .body(email_content.clone())
                .map_err(MyError::EmailError)?;

            match mailer.send(&email) {
                Ok(_) => println!("Email sent successfully to: {}", business.email),
                Err(e) => eprintln!("Could not send email to: {}: {:?}", business.email, e),
            }

            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        } else {
            println!("Reached the daily limit of max emails sent.");
            break;
        }
    }

    Ok(())
}