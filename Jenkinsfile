@Library('socrata-pipeline-library@10.2.1') _

commonPipeline(
    defaultBuildWorker: 'worker-java-multi-pg13',
    jobName: 'query-coordinator',
    languageOptions: [
        scala: [
            crossCompile: true,
        ],
    ],
    numberOfBuildsToKeep: 50,
    projects: [
        [
          name: 'query-coordinator',
          compiled: true,
          deploymentEcosystem: 'ecs',
          docker: [
              buildContext: 'query-coordinator/docker'
          ],
          language: 'scala',
          type: 'service',
        ]
  ],
  teamsChannelWebhookId: 'WORKFLOW_EGRESS_AUTOMATION',
)
