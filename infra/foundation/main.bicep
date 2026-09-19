targetScope = 'resourceGroup'

@description('Azure region for all foundation resources.')
param location string = resourceGroup().location

@description('Prefix used for globally and locally named Mercury resources.')
@minLength(2)
@maxLength(24)
param namePrefix string = 'mercury-dev'

@description('Globally unique lowercase ACR name.')
@minLength(5)
@maxLength(50)
param registryName string

@description('Container Apps managed environment name.')
param containerAppsEnvironmentName string = '${namePrefix}-env'

@description('Globally unique PostgreSQL Flexible Server name.')
param postgresServerName string

@description('PostgreSQL database name.')
param postgresDatabaseName string = 'mercury'

@description('PostgreSQL administrator login name.')
param postgresAdministratorLogin string = 'mercuryadmin'

@secure()
@description('PostgreSQL administrator password. Supply at deployment time; never commit it.')
param postgresAdministratorPassword string

@description('PostgreSQL major version.')
@allowed([
  '16'
  '17'
])
param postgresVersion string = '16'

@description('Cost-conscious PostgreSQL compute tier.')
@allowed([
  'Burstable'
  'GeneralPurpose'
  'MemoryOptimized'
])
param postgresSkuTier string = 'Burstable'

@description('PostgreSQL compute SKU.')
param postgresSkuName string = 'Standard_B1ms'

@description('PostgreSQL provisioned storage in GiB.')
@minValue(32)
param postgresStorageSizeGB int = 32

@description('Common Azure resource tags.')
param tags object = {
  application: 'mercury'
  environment: 'development'
}

resource registry 'Microsoft.ContainerRegistry/registries@2023-07-01' = {
  name: registryName
  location: location
  tags: tags
  sku: {
    name: 'Basic'
  }
  properties: {
    adminUserEnabled: false
    publicNetworkAccess: 'Enabled'
  }
}

// Omitting a Log Analytics workspace keeps the development footprint small.
resource containerAppsEnvironment 'Microsoft.App/managedEnvironments@2025-01-01' = {
  name: containerAppsEnvironmentName
  location: location
  tags: tags
  properties: {}
}

resource postgresServer 'Microsoft.DBforPostgreSQL/flexibleServers@2025-08-01' = {
  name: postgresServerName
  location: location
  tags: tags
  sku: {
    name: postgresSkuName
    tier: postgresSkuTier
  }
  properties: {
    administratorLogin: postgresAdministratorLogin
    administratorLoginPassword: postgresAdministratorPassword
    version: postgresVersion
    authConfig: {
      activeDirectoryAuth: 'Disabled'
      passwordAuth: 'Enabled'
    }
    backup: {
      backupRetentionDays: 7
      geoRedundantBackup: 'Disabled'
    }
    highAvailability: {
      mode: 'Disabled'
    }
    network: {
      publicNetworkAccess: 'Enabled'
    }
    storage: {
      autoGrow: 'Enabled'
      storageSizeGB: postgresStorageSizeGB
    }
  }
}

resource mercuryDatabase 'Microsoft.DBforPostgreSQL/flexibleServers/databases@2025-08-01' = {
  parent: postgresServer
  name: postgresDatabaseName
  properties: {
    charset: 'UTF8'
    collation: 'en_US.utf8'
  }
}

// This development rule admits Azure-hosted callers; TLS still protects transit.
resource allowAzureServices 'Microsoft.DBforPostgreSQL/flexibleServers/firewallRules@2025-08-01' = {
  parent: postgresServer
  name: 'AllowAzureServices'
  properties: {
    startIpAddress: '0.0.0.0'
    endIpAddress: '0.0.0.0'
  }
}

output registryName string = registry.name
output registryLoginServer string = registry.properties.loginServer
output containerAppsEnvironmentName string = containerAppsEnvironment.name
output containerAppsEnvironmentId string = containerAppsEnvironment.id
output postgresServerName string = postgresServer.name
output postgresFullyQualifiedDomainName string = postgresServer.properties.fullyQualifiedDomainName
output postgresDatabaseName string = mercuryDatabase.name
output postgresAdministratorLogin string = postgresAdministratorLogin
