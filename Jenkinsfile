@Library('socrata-pipeline-library@5.0.0') _

commonPipeline(
  defaultBuildWorker: 'build-worker-pg13',
  jobName: 'query-coordinator',
  language: 'scala',
  languageVersion: '2.12',
  numberOfBuildsToKeep: 50,
  projects: [
    [
      name: 'query-coordinator',
      type: 'service',
      deploymentEcosystem: 'marathon-mesos',
    ]
  ],
  projectWorkingDirectory: 'query-coordinator',
  teamsChannelWebhookId: 'WORKFLOW_IQ',
  scalaSrcJar: 'query-coordinator/target/query-coordinator-assembly.jar',
  scalaSubprojectName: 'queryCoordinator',
)
