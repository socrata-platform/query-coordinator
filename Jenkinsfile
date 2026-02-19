@Library('socrata-pipeline-library@9.9.1') _

commonPipeline(
    defaultBuildWorker: 'build-worker-pg13',
    jobName: 'query-coordinator',
    language: 'scala',
    languageOptions: [
        crossCompile: true,
    ],
    numberOfBuildsToKeep: 50,
    projects: [
        [
          name: 'query-coordinator',
          compiled: true,
          deploymentEcosystem: 'ecs',
          paths: [
              dockerBuildContext: 'query-coordinator/docker'
          ],
          type: 'service',
        ]
  ],
  teamsChannelWebhookId: 'WORKFLOW_EGRESS_AUTOMATION',
)
