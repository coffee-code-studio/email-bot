use std::fs::File;
use std::io::Write;
use std::io;
use std::collections::HashSet;
use std::env;
use dotenvy::dotenv;
use reqwest;
use regex::Regex;
use scraper::{Html, Selector};
use serde::{Serialize, Deserialize};
use serde_json;
use lettre::{Message, SmtpTransport, Transport, message::header::ContentType};
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
pub enum BotError {
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
}

fn current_day() -> String {
    Utc::now().format("%Y-%m-%d").to_string()
}

fn check_update_email_count(con: &mut redis::Connection, max_emails_per_day: usize) -> Result<bool, BotError> {
    let today = current_day();
    let key = format!("emails_sent:{}", today);
    let mut retry_count = 0;
    let max_retries = 5;

    loop {
        match con.get::<_, Option<isize>>(&key) {
            Ok(Some(count)) => {
                if count < max_emails_per_day as isize {
                    let _: () = con.incr(&key, 1).map_err(BotError::RedisError)?;
                    if count == 0 {
                        let _: () = con.expire(&key, 86400).map_err(BotError::RedisError)?;
                    }
                    return Ok(true);
                } else {
                    return Ok(false);
                }
            },
            Ok(None) => {
                let _: () = con.set(&key, 1).map_err(BotError::RedisError)?;
                let _: () = con.expire(&key, 86400).map_err(BotError::RedisError)?;
                return Ok(true);
            },
            Err(err) => {
                if retry_count >= max_retries {
                    return Err(BotError::RedisError(err));
                } else {
                    eprintln!("Redis failed, retrying... (Attempt: {})", retry_count + 1);
                    retry_count += 1;
                    thread::sleep(Duration::from_millis(500));
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), BotError> {
    let client = reqwest::Client::new();
    let mut processed_emails: HashSet<String> = HashSet::new();
    let mut page_number = 1;
    let mut has_more_pages = true;
    let mut businesses: Vec<Business> = Vec::new();

    // Establish Redis connection
    let redis_client = redis::Client::open("redis://127.0.0.1/").map_err(BotError::RedisError)?;
    let mut redis_con = redis_client.get_connection().map_err(BotError::RedisError)?;

    let max_emails_per_day = 400;

    while has_more_pages {
        let list_page_url = format!(
            "https://www.yellowpages.com/search?search_terms=Electricians&geo_location_terms=Columbus%2C+OH&page={}",
            page_number
        );

        let list_page_response = client.get(&list_page_url)
            .send()
            .await
            .map_err(BotError::NetworkError)?
            .text()
            .await
            .map_err(BotError::NetworkError)?;
        
        let list_page_document = Html::parse_document(&list_page_response);
        let business_link_selector = Selector::parse("a.business-name").unwrap();
        let business_links: Vec<_> = list_page_document.select(&business_link_selector).collect();

        if business_links.is_empty() {
            break;
        }

        for link_element in business_links {
            if let Some(href) = link_element.value().attr("href") {
                let detail_url = format!("https://www.yellowpages.com{}", href);
                let detail_page_response = client.get(&detail_url)
                    .send()
                    .await
                    .map_err(BotError::NetworkError)?
                    .text()
                    .await
                    .map_err(BotError::NetworkError)?;
                
                let detail_page_document = Html::parse_document(&detail_page_response);
                let email_selector = Selector::parse("a.email-business").unwrap();
        
                if let Some(email_element) = detail_page_document.select(&email_selector).next() {
                    let email = if let Some(email_href) = email_element.value().attr("href") {
                        if email_href.starts_with("mailto:") {
                            let re = Regex::new(r"mailto:([^?]+)").unwrap();
                            if let Some(caps) = re.captures(email_href) {
                                caps.get(1).map_or("", |m| m.as_str()).to_string()
                            } else {
                                "".to_string()
                            }
                        } else {
                            email_href.to_string()
                        }
                    } else {
                        email_element.inner_html()
                    };
                
                    if !processed_emails.contains(&email) {
                        processed_emails.insert(email.clone());
        
                        println!("Business URL: {}", detail_url);
                        println!("Business Email: {}", email);
        
                        businesses.push(Business {
                            url: detail_url,
                            email: email.clone(),
                        });
                    } else {
                        println!("Duplicate email found, skipping: {}", email);
                    }
                }
        
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }

        page_number += 1;
    }

    let json_data = serde_json::to_string_pretty(&businesses).map_err(BotError::DataParseError)?;
    let mut file = File::create("business_emails.json").map_err(BotError::IOError)?;
    file.write_all(json_data.as_bytes()).map_err(BotError::IOError)?;

    dotenv().expect(".env file not found");

    let email_sender = "coffeecodestudio.dev@gmail.com";
    let email_password = env::var("EMAIL_PASSWORD").expect("EMAIL_PASSWORD not set");
    let creds = Credentials::new(email_sender.to_string(), email_password);
    let mailer = SmtpTransport::relay("smtp.gmail.com")?.credentials(creds).build();
    
    let subject = "Grow Your Business with Coffee Code Studio - Special Offer Inside!".to_string();

    let email_template = EmailTemplate {
        subject: subject.clone(),
    };

    let email_content = email_template.render().map_err(BotError::TemplateError)?;

    println!("Email content preview:");
    println!("Subject: {}", subject);
    println!("Content: {}", email_content);
    println!("-------------------------");

    println!("Do you want to proceed with sending emails? (yes/no):");
    let mut confirmation = String::new();
    std::io::stdin().read_line(&mut confirmation).map_err(BotError::IOError)?;
    if confirmation.trim().to_lowercase() != "yes" {
        println!("Aborted by user.");
        return Ok(());
    }

    for business in &businesses {
        if !business.email.contains('@') {
            println!("Invalid email skipped: {}", business.email);
            continue;
        }

        if check_update_email_count(&mut redis_con, max_emails_per_day)? {
            let email = Message::builder()
                .from(email_sender.parse().unwrap())
                .to(business.email.parse().unwrap())
                .subject(&subject)
                .header(ContentType::TEXT_HTML) 
                .body(email_content.clone())
                .map_err(BotError::EmailError)?;


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