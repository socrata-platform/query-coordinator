@Library('socrata-pipeline-library@0.2.0') _

commonServicePipeline(
  defaultBuildWorker: 'build-worker-pg13',
  deploymentEcosystem: 'marathon-mesos',
  language: 'scala',
  languageVersion: '2.12',
  numberOfBuildsToKeep: 50,
  projectName: 'query-coordinator',
  projectWorkingDirectory: 'query-coordinator',
  teamsChannelWebhookId: 'WORKFLOW_IQ',
  scalaSrcJar: 'query-coordinator/target/query-coordinator-assembly.jar',
  scalaSubprojectName: 'queryCoordinator',
)
