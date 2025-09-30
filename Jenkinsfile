@Library('socrata-pipeline-library@9.7.0') _

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
      type: 'service',
      deploymentEcosystem: 'marathon-mesos',
      compiled: true,
      paths: [
        dockerBuildContext: 'query-coordinator/docker'
      ]
    ]
  ],
  teamsChannelWebhookId: 'WORKFLOW_EGRESS_AUTOMATION',
)
