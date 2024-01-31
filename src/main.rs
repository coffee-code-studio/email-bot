use std::fs::File;
use std::io::Write;
use reqwest;
use scraper::{Html, Selector};
use serde::{Serialize, Deserialize};
use serde_json;
use lettre::{Message, SmtpTransport, Transport};
use lettre::transport::smtp::authentication::Credentials;
use askama::Template;

#[derive(Serialize, Deserialize, Debug)]
struct Business {
    url: String,
    email: String,
}

#[derive(Template)]
#[template(path = "email_template.html")]
struct EmailTemplate {
    subject: String,
    your_name: String,
    your_position: String,
    contact_information: String,
    website_url: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let mut page_number = 1;
    let mut has_more_pages = true;
    let mut businesses: Vec<Business> = Vec::new();

    while has_more_pages {
        let list_page_url = format!(
            "https://www.yellowpages.com/search?search_terms=event+planning&geo_location_terms=Columbus%2C+OH&page={}",
            page_number
        );

        let list_page_response = client.get(&list_page_url).send().await?.text().await?;
        let list_page_document = Html::parse_document(&list_page_response);
        let business_link_selector = Selector::parse("a.business-name").unwrap();
        let business_links: Vec<_> = list_page_document.select(&business_link_selector).collect();

        if business_links.is_empty() {
            has_more_pages = false;
            break;
        }

        for link_element in business_links {
            if let Some(href) = link_element.value().attr("href") {
                let detail_url = format!("https://www.yellowpages.com{}", href);
                let detail_page_response = client.get(&detail_url).send().await?.text().await?;
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

    let json_data = serde_json::to_string_pretty(&businesses)?;
    let mut file = File::create("business_emails.json")?;
    file.write_all(json_data.as_bytes())?;

    let email_sender = "coffeecodestudio.dev@gmail.com";
    let creds = Credentials::new(email_sender.to_string(), "<add password>".to_string());
    let mailer = SmtpTransport::relay("smtp.gmail.com")?
        .credentials(creds)
        .build();

    let subject = "Grow Your Business with Coffee Code Studio - Special Offer Inside!".to_string();
    let your_name = "Tim Reed".to_string();
    let your_position = "Lead Developer".to_string();
    let contact_information = "coffeecodestudio.dev@gmail.com".to_string();
    let website_url = "https://coffeecodestudio.com/".to_string();

    let email_template = EmailTemplate {
        subject: subject.clone(),
        your_name,
        your_position,
        contact_information,
        website_url,
    };

    let email_content = email_template.render()?;

    for business in &businesses {
        if !business.email.contains('@') {
            println!("Invalid email skipped: {}", business.email);
            continue;
        }

        let email = Message::builder()
            .from(email_sender.parse().unwrap())
            .to(business.email.parse().unwrap())
            .subject(&subject)
            .body(email_content.clone())
            .unwrap();

        match mailer.send(&email) {
            Ok(_) => println!("Email sent successfully to: {}", business.email),
            Err(e) => eprintln!("Could not send email to: {}: {:?}", business.email, e),
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }

    Ok(())
}