# Ejemplo para enviar eventos variados con curl en PowerShell
$events = @(
    @{ 
        url = "http://localhost:8000/api/events/impression"
        body = @{
            impression_id = "imp-001"
            user_ip = "192.168.1.10"
            user_agent = "Mozilla/5.0"
            timestamp = (Get-Date).ToUniversalTime().ToString("o")
            state = "NY"
            search_keywords = "tennis shoes"
            session_id = "session-001"
            ads = @(
                @{
                    advertiser = @{advertiser_id = "adv-001"; advertiser_name = "Adidas"}
                    campaign = @{campaign_id = "camp-001"; campaign_name = "Summer Sale"}
                    ad = @{ad_id = "ad-001"; ad_name = "Adidas Runner"; ad_text = "Best shoes"; ad_link = "https://example.com/adidas"; ad_position = 1; ad_format = "banner_300x250"}
                }
            )
        }
    }
    @{ 
        url = "http://localhost:8000/api/events/click"
        body = @{
            click_id = "click-001"
            impression_id = "imp-001"
            timestamp = (Get-Date).AddMinutes(-5).ToUniversalTime().ToString("o")
            clicked_ad = @{ad_id = "ad-001"; ad_position = 1; click_coordinates = @{x=100; y=150; normalized_x=0.33; normalized_y=0.5}; time_to_click=10}
            user_info = @{user_ip = "192.168.1.10"; state = "NY"; session_id = "session-001"}
        }
    }
    @{ 
        url = "http://localhost:8000/api/events/conversion"
        body = @{
            conversion_id = "conv-001"
            click_id = "click-001"
            impression_id = "imp-001"
            conversion_type = "purchase"
            conversion_value = 120.0
            conversion_currency = "USD"
            conversion_attributes = @{order_id = "order-001"; items = @(@{product_id = "prod-001"; quantity = 2; unit_price = 60.0})}
            attribution_info = @{time_to_convert = 300; attribution_model = "last_click"; conversion_path = @(@{event_type="impression"; timestamp = (Get-Date).AddMinutes(-30).ToUniversalTime().ToString("o")}, @{event_type="click"; timestamp = (Get-Date).AddMinutes(-10).ToUniversalTime().ToString("o")})}
            user_info = @{user_ip = "192.168.1.10"; state = "NY"; session_id = "session-001"}
            timestamp = (Get-Date).ToUniversalTime().ToString("o")
        }
    }
)

foreach ($event in $events) {
    $json = $event.body | ConvertTo-Json -Depth 10
    curl -X POST $event.url -H "Content-Type: application/json" -d $json
    Write-Host "Evento enviado a $($event.url)"
}
