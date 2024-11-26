@Library('socrata-pipeline-library')

Map pipelineParams = [
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
]

commonServicePipeline(pipelineParams)
