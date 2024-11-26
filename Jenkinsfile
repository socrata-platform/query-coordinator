@Library('socrata-pipeline-library')

Map pipelineParams = [
  projectName: 'query-coordinator',
  language: 'scala',
  defaultBuildWorker: 'build-worker-pg13',
  numberOfBuildsToKeep: 50,
  teamsChannelWebhookId: 'WORKFLOW_IQ',
  projectWorkingDirectory: 'query-coordinator',
  languageVersion: '2.12',
  scalaSubprojectName: "queryCoordinator",
  scalaSrcJar: "query-coordinator/target/query-coordinator-assembly.jar",
  deploymentEcosystem: 'marathon-mesos',
]

commonServicePipeline(pipelineParams)
