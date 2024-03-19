@Library('socrata-pipeline-library')

import com.socrata.ReleaseMetadataService
def rmsSupportedEnvironment = com.socrata.ReleaseMetadataService.SupportedEnvironment

String service = 'query-coordinator'
String project_wd = service
boolean isPr = env.CHANGE_ID != null
boolean lastStage

// Utility Libraries
def sbtbuild = new com.socrata.SBTBuild(steps, service, project_wd)
def dockerize = new com.socrata.Dockerize(steps, service, BUILD_NUMBER)
def releaseTag = new com.socrata.ReleaseTag(steps, service)

pipeline {
  options {
    ansiColor('xterm')
  }
  parameters {
    booleanParam(name: 'RELEASE_BUILD', defaultValue: false, description: 'Are we building a release candidate?')
    booleanParam(name: 'RELEASE_DRY_RUN', defaultValue: false, description: 'To test out the release build without creating a new tag.')
    string(name: 'RELEASE_NAME', defaultValue: '', description: 'For release builds, the release name which is used for the git tag and the deploy tag.')
    string(name: 'AGENT', defaultValue: 'build-worker', description: 'Which build agent to use?')
    string(name: 'BRANCH_SPECIFIER', defaultValue: 'origin/main', description: 'Use this branch for building the artifact.')
  }
  agent {
    label params.AGENT
  }
  environment {
    WEBHOOK_ID = 'WEBHOOK_IQ'
  }
  stages {
    stage('Release Tag') {
      when {
        expression { return params.RELEASE_BUILD }
      }
      steps {
        script {
          lastStage = env.STAGE_NAME
          if (params.RELEASE_DRY_RUN) {
            echo 'DRY RUN: Skipping release tag creation'
          }
          else {
            env.GIT_TAG = releaseTag.create(params.RELEASE_NAME)
          }
        }
      }
    }
    stage('Build') {
      steps {
        script {
          lastStage = env.STAGE_NAME
          sbtbuild.setScalaVersion("2.12")
          sbtbuild.setSubprojectName("queryCoordinator")
          sbtbuild.setSrcJar("query-coordinator/target/query-coordinator-assembly.jar")
          echo "Building sbt project..."
          sbtbuild.build()
        }
      }
    }
    stage('Dockerize') {
      when {
        not { expression { isPr } }
      }
      steps {
        script {
          lastStage = env.STAGE_NAME
          env.REGISTRY_PUSH = 'internal'
          env.DOCKER_TAG = dockerize.docker_build('STAGING', env.GIT_COMMIT, sbtbuild.getDockerPath(), sbtbuild.getDockerArtifact(), env.REGISTRY_PUSH)
          currentBuild.description = env.DOCKER_TAG
        }
      }
      post {
        success {
          script {
            if (params.RELEASE_BUILD && !params.RELEASE_DRY_RUN){
              Map buildInfo = [
                "project_id": service,
                "build_id": env.DOCKER_TAG,
                "release_id": params.RELEASE_NAME,
                "git_tag": env.GIT_TAG,
              ]
              createBuild(
                buildInfo,
                rmsSupportedEnvironment.production
              )
            }
          }
        }
      }
    }
    stage('Deploy') {
      when {
        not { expression { isPr } }
        not { expression { return params.RELEASE_BUILD } }
      }
      steps {
        script {
          lastStage = env.STAGE_NAME
          // uses env.DOCKER_TAG and deploys to staging by default
          marathonDeploy(serviceName: service)
        }
      }
    }
  }
  post {
    failure {
      script {
        if (!isPr) {
          teamsMessage(message: "Build [${currentBuild.fullDisplayName}](${env.BUILD_URL}) has failed in stage ${lastStage}", webhookCredentialID: WEBHOOK_ID)
        }
      }
    }
  }
}
