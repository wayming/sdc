# Stock Data Collector (SDC)

A distributed system for collecting and managing stock market data with multiple data sources integration.

## System Components

- **Collector Service**: Main service for collecting stock data from various sources
- **Scraper Service**: Distributed HTML scraping service with multiple replicas
- **PostgreSQL**: Primary data storage
- **Redis**: Caching and queue management
- **OpenBB Integration**: Integration with OpenBB Platform
- **pgAdmin**: Database management interface
- **Nginx**: Reverse proxy for OpenBB interface

## Prerequisites

- Docker and Docker Compose
- Docker Swarm (for overlay network)
- Required secrets configured (see Configuration section)

## Configuration

### Required Secrets

The following Docker secrets need to be configured:

```bash
# Create required secrets
echo "your_password" | docker secret create pg_pass_file -
echo "your_password" | docker secret create pgadmin_pass_file -
echo "your_access_key" | docker secret create market_stack_access_key -
echo "your_proxy_password" | docker secret create proxy_pass_file -
```

### Environment Variables

Key environment variables for the collector service:
- `PGHOST`, `PGPORT`, `PGUSER`, `PGDATABASE`: PostgreSQL connection settings
- `REDISHOST`, `REDISPORT`: Redis connection settings
- `MSACCESSKEYFILE`: Market Stack API access key file path
- `PROXYUSER`: Proxy username for web scraping

## Getting Started

1. Start the Docker Swarm mode if not already initialized:
   ```bash
   docker swarm init
   ```

2. Deploy the stack:
   ```bash
   docker-compose -f docker-compose.debug.yml up -d
   ```

## Services

### Collector (stock_data_collector)
- Main data collection service
- Handles multiple data source integrations
- Ports: 5001

### Scraper
- Distributed HTML scraping service
- 10 replicas for parallel processing
- Mounted volume: `/htmlscraper`

### PostgreSQL
- Primary data storage
- Port: 5432
- Data persistence: `./pg_data`
- Logs: `./pg_log`

### pgAdmin
- Database management interface
- Port: 5050
- Access URL: `http://localhost:5050`
- Default email: wayming.z@gmail.com

### Redis
- Caching and queue management
- Port: 6379
- Data persistence: `./redis_data`

### OpenBB
- OpenBB Platform integration
- Port: 6900
- Custom configuration in `./openbb_platform`

### Nginx
- Reverse proxy for OpenBB interface
- Port: 4000
- Configuration: `./nginx.conf`

## Project Structure

```
sdc/
├── cache/           # Cache management
├── collector/       # Main collector service
├── common/         # Shared utilities
├── config/         # Configuration management
├── data/           # Data files
├── dbloader/       # Database loading utilities
├── docker/         # Docker configurations
├── htmlscraper/    # Web scraping module
└── utils/          # Utility functions
```

## Development

The project uses Go modules for dependency management. Main development files are mounted into containers for easy development and debugging.

## License

[Add your license information here]
