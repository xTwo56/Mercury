targetScope = 'resourceGroup'

@description('Azure region; it must match the foundation deployment.')
param location string = resourceGroup().location

@description('Existing Azure Container Registry name.')
param registryName string

@description('Existing Container Apps managed environment name.')
param containerAppsEnvironmentName string

@description('Tag of the Mercury runtime image. Mutable latest tags are rejected.')
param mercuryImageTag string

@description('Tag of the Mercury migration image. Mutable latest tags are rejected.')
param migrationImageTag string

@secure()
@description('TLS-enabled PostgreSQL URL stored as a Container Apps secret.')
param databaseUrl string

@description('Public API Container App name.')
param apiAppName string = 'mercury-api'

@description('Private worker Container App name.')
param workerAppName string = 'mercury-worker'

@description('Private scheduler Container App name.')
param schedulerAppName string = 'mercury-scheduler'

@description('Manually triggered migration job name.')
param migrationJobName string = 'mercury-migrate'

@description('Shared runtime managed identity name.')
param runtimeIdentityName string = 'mercury-runtime'

@description('Minimum API replicas.')
@minValue(0)
param apiMinReplicas int = 1

@description('Maximum API replicas.')
@minValue(1)
param apiMaxReplicas int = 3

@description('Worker replicas. Keep at one initially; PostgreSQL locking supports later scale-out.')
@minValue(1)
param workerReplicaCount int = 1

@description('Scheduler replicas. Exactly one is intentional for the initial deployment.')
@allowed([
  1
])
param schedulerReplicaCount int = 1

@description('Common Azure resource tags.')
param tags object = {
  application: 'mercury'
  environment: 'development'
}

assert apiReplicaRangeIsValid = apiMaxReplicas >= apiMinReplicas
assert mercuryTagIsImmutable = toLower(trim(mercuryImageTag)) != 'latest' && !empty(trim(mercuryImageTag))
assert migrationTagIsImmutable = toLower(trim(migrationImageTag)) != 'latest' && !empty(trim(migrationImageTag))
assert databaseRequiresTLS = contains(toLower(databaseUrl), 'sslmode=require') || contains(toLower(databaseUrl), 'sslmode=verify-ca') || contains(toLower(databaseUrl), 'sslmode=verify-full')

resource registry 'Microsoft.ContainerRegistry/registries@2023-07-01' existing = {
  name: registryName
}

resource containerAppsEnvironment 'Microsoft.App/managedEnvironments@2025-01-01' existing = {
  name: containerAppsEnvironmentName
}

resource runtimeIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: runtimeIdentityName
  location: location
  tags: tags
}

var acrPullRoleDefinitionId = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '7f951dda-4ed3-4680-a7ca-43fe172d538d')

resource runtimeAcrPull 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(registry.id, runtimeIdentity.id, acrPullRoleDefinitionId)
  scope: registry
  properties: {
    principalId: runtimeIdentity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: acrPullRoleDefinitionId
  }
}

var runtimeImage = '${registry.properties.loginServer}/mercury:${mercuryImageTag}'
var migrationImage = '${registry.properties.loginServer}/mercury-migrate:${migrationImageTag}'
var registryConfiguration = [
  {
    server: registry.properties.loginServer
    identity: runtimeIdentity.id
  }
]
var databaseSecrets = [
  {
    name: 'database-url'
    value: databaseUrl
  }
]
var databaseEnvironment = {
  name: 'MERCURY_DATABASE_URL'
  secretRef: 'database-url'
}

resource api 'Microsoft.App/containerApps@2025-01-01' = {
  name: apiAppName
  location: location
  tags: tags
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${runtimeIdentity.id}': {}
    }
  }
  properties: {
    environmentId: containerAppsEnvironment.id
    configuration: {
      activeRevisionsMode: 'Single'
      ingress: {
        external: true
        allowInsecure: false
        targetPort: 8080
        transport: 'http'
      }
      registries: registryConfiguration
      secrets: databaseSecrets
    }
    template: {
      containers: [
        {
          name: 'mercury-api'
          image: runtimeImage
          env: [
            databaseEnvironment
            {
              name: 'MERCURY_ROLE'
              value: 'api'
            }
            {
              name: 'MERCURY_HTTP_LISTEN_ADDRESS'
              value: ':8080'
            }
          ]
          resources: {
            cpu: json('0.25')
            memory: '0.5Gi'
          }
        }
      ]
      scale: {
        minReplicas: apiMinReplicas
        maxReplicas: apiMaxReplicas
      }
    }
  }
  dependsOn: [runtimeAcrPull]
}

resource worker 'Microsoft.App/containerApps@2025-01-01' = {
  name: workerAppName
  location: location
  tags: tags
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${runtimeIdentity.id}': {}
    }
  }
  properties: {
    environmentId: containerAppsEnvironment.id
    configuration: {
      activeRevisionsMode: 'Single'
      registries: registryConfiguration
      secrets: databaseSecrets
    }
    template: {
      containers: [
        {
          name: 'mercury-worker'
          image: runtimeImage
          env: [
            databaseEnvironment
            {
              name: 'MERCURY_ROLE'
              value: 'worker'
            }
          ]
          resources: {
            cpu: json('0.25')
            memory: '0.5Gi'
          }
        }
      ]
      scale: {
        minReplicas: workerReplicaCount
        maxReplicas: workerReplicaCount
      }
    }
  }
  dependsOn: [runtimeAcrPull]
}

resource scheduler 'Microsoft.App/containerApps@2025-01-01' = {
  name: schedulerAppName
  location: location
  tags: tags
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${runtimeIdentity.id}': {}
    }
  }
  properties: {
    environmentId: containerAppsEnvironment.id
    configuration: {
      activeRevisionsMode: 'Single'
      registries: registryConfiguration
      secrets: databaseSecrets
    }
    template: {
      containers: [
        {
          name: 'mercury-scheduler'
          image: runtimeImage
          env: [
            databaseEnvironment
            {
              name: 'MERCURY_ROLE'
              value: 'scheduler'
            }
          ]
          resources: {
            cpu: json('0.25')
            memory: '0.5Gi'
          }
        }
      ]
      scale: {
        minReplicas: schedulerReplicaCount
        maxReplicas: schedulerReplicaCount
      }
    }
  }
  dependsOn: [runtimeAcrPull]
}

resource migrationJob 'Microsoft.App/jobs@2025-01-01' = {
  name: migrationJobName
  location: location
  tags: tags
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${runtimeIdentity.id}': {}
    }
  }
  properties: {
    environmentId: containerAppsEnvironment.id
    configuration: {
      triggerType: 'Manual'
      replicaTimeout: 1800
      replicaRetryLimit: 1
      manualTriggerConfig: {
        parallelism: 1
        replicaCompletionCount: 1
      }
      registries: registryConfiguration
      secrets: databaseSecrets
    }
    template: {
      containers: [
        {
          name: 'mercury-migrate'
          image: migrationImage
          env: [databaseEnvironment]
          resources: {
            cpu: json('0.25')
            memory: '0.5Gi'
          }
        }
      ]
    }
  }
  dependsOn: [runtimeAcrPull]
}

output apiUrl string = 'https://${api.properties.configuration.ingress.fqdn}'
output migrationJobName string = migrationJob.name
output runtimeIdentityPrincipalId string = runtimeIdentity.properties.principalId
