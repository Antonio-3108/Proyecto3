# Enviar evento de impresión
Invoke-RestMethod -Uri "http://localhost:8000/api/events/impression" -Method POST -ContentType "application/json" -Body @"
{
  "impression_id": "imp-test-001",
  "user_ip": "192.168.1.1",
  "user_agent": "Mozilla/5.0",
  "timestamp": "2025-05-14T12:00:00Z",
  "state": "CA",
  "search_keywords": "running shoes",
  "session_id": "session-abc123",
  "ads": [
    {
      "advertiser": {
        "advertiser_id": "adv-789",
        "advertiser_name": "Nike Inc."
      },
      "campaign": {
        "campaign_id": "camp-456",
        "campaign_name": "Fall Collection 2023"
      },
      "ad": {
        "ad_id": "ad-123",
        "ad_name": "Air Max Pro",
        "ad_text": "New Air Max Pro - Limited Edition",
        "ad_link": "https://example.com/airmaxpro",
        "ad_position": 1,
        "ad_format": "banner_728x90"
      }
    }
  ]
}
"@

Write-Host "`nEvento Impresión enviado`n"

# Enviar evento de click
Invoke-RestMethod -Uri "http://localhost:8000/api/events/click" -Method POST -ContentType "application/json" -Body @"
{
  "click_id": "click-test-001",
  "impression_id": "imp-test-001",
  "timestamp": "2025-05-14T12:01:00Z",
  "clicked_ad": {
    "ad_id": "ad-123",
    "ad_position": 1,
    "click_coordinates": {
      "x": 250,
      "y": 400,
      "normalized_x": 0.65,
      "normalized_y": 0.80
    },
    "time_to_click": 60
  },
  "user_info": {
    "user_ip": "192.168.1.1",
    "state": "CA",
    "session_id": "session-abc123"
  }
}
"@

Write-Host "`nEvento Click enviado`n"

# Enviar evento de conversión
Invoke-RestMethod -Uri "http://localhost:8000/api/events/conversion" -Method POST -ContentType "application/json" -Body @"
{
  "conversion_id": "conv-test-001",
  "click_id": "click-test-001",
  "impression_id": "imp-test-001",
  "timestamp": "2025-05-14T12:10:00Z",
  "conversion_type": "purchase",
  "conversion_value": 59.99,
  "conversion_currency": "USD",
  "conversion_attributes": {
    "order_id": "order-xyz987",
    "items": [
      {
        "product_id": "prod-135",
        "quantity": 1,
        "unit_price": 59.99
      }
    ]
  },
  "attribution_info": {
    "time_to_convert": 600,
    "attribution_model": "last_click",
    "conversion_path": [
      {
        "event_type": "impression",
        "timestamp": "2025-05-14T12:00:00Z"
      },
      {
        "event_type": "click",
        "timestamp": "2025-05-14T12:01:00Z"
      }
    ]
  },
  "user_info": {
    "user_ip": "192.168.1.1",
    "state": "CA",
    "session_id": "session-abc123"
  }
}
"@

Write-Host "`nEvento Conversión enviado`n"
