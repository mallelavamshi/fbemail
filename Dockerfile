version: '3.8'

services:
  email-scraper:
    container_name: email-scraper
    build: .
    volumes:
      - ./uploaded_files:/app/uploaded_files
      - ./outputs:/app/outputs
      - ./jobs:/app/jobs
      - ./logs:/app/logs
    restart: unless-stopped
    environment:
      - PYTHONUNBUFFERED=1
    networks:
      - email-scraper-network

  nginx:
    image: nginx:alpine
    container_name: email-scraper-nginx
    ports:
      - "80:80"       # Standard HTTP port
      - "443:443"     # Standard HTTPS port
    volumes:
      - ./nginx.conf:/etc/nginx/conf.d/default.conf:ro
      - /etc/letsencrypt:/etc/letsencrypt:ro
    depends_on:
      - email-scraper
    restart: unless-stopped
    networks:
      - email-scraper-network

networks:
  email-scraper-network:
    driver: bridge
