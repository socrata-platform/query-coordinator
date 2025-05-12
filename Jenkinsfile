@Library('socrata-pipeline-library@generalize-sbt-to-work-for-multiple-projects') _

commonPipeline(
  defaultBuildWorker: 'build-worker-pg13',
  jobName: 'query-coordinator',
  language: 'scala',
  languageOptions: [
      crossCompile: true,
      isMultiProjectRepository: false,
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
  projectWorkingDirectory: 'query-coordinator',
  teamsChannelWebhookId: 'WORKFLOW_IQ',
)
