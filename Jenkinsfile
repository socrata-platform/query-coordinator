@Library('socrata-pipeline-library')

import com.socrata.ReleaseMetadataService
def rmsSupportedEnvironment = com.socrata.ReleaseMetadataService.SupportedEnvironment

String service = 'query-coordinator'
String project_wd = service
boolean isPr = env.CHANGE_ID != null
boolean isHotfix = isHotfixBranch(env.BRANCH_NAME)
boolean isReleaseRebuild = false
boolean skip = false
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
    string(name: 'AGENT', defaultValue: 'build-worker', description: 'Which build agent to use?')
    string(name: 'BRANCH_SPECIFIER', defaultValue: 'origin/main', description: 'Use this branch for building the artifact.')
    string(name: 'RELEASE_NAME', defaultValue: '', description: 'For release builds, the release name which is used for the git tag and the deploy tag.')
    booleanParam(name: 'RELEASE_BUILD', defaultValue: false, description: 'Are we building a release candidate?')
    booleanParam(name: 'RELEASE_DRY_RUN', defaultValue: false, description: 'To test out the release build without creating a new tag.')
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
          env.GIT_TAG = releaseTag.getFormattedTag(params.RELEASE_NAME)
          if (releaseTag.doesReleaseTagExist(params.RELEASE_NAME)) {
            isReleaseRebuild = true
            echo "REBUILD: Tag ${env.GIT_TAG} already exists -- checking out the tag"
            releaseTag.checkoutTag(params.RELEASE_NAME)
            return
          }
          if (params.RELEASE_DRY_RUN) {
            echo "DRY RUN: Would have created ${env.GIT_TAG} and pushed it to the repo"
            return
          }
          releaseTag.create(params.RELEASE_NAME)
        }
      }
    }
    stage('Hotfix Tag') {
      when {
        expression { isHotfix }
      }
      steps {
        script {
          lastStage = env.STAGE_NAME
          if (releaseTag.noCommitsOnHotfixBranch(env.BRANCH_NAME)) {
            skip = true
            echo "SKIP: Skipping the rest of the build because there are no commits on the hotfix branch yet"
            return
          }
          env.CURRENT_RELEASE_NAME = releaseTag.getReleaseName(env.BRANCH_NAME)
          env.HOTFIX_NAME = releaseTag.getHotfixName(env.CURRENT_RELEASE_NAME)
          env.GIT_TAG = releaseTag.create(env.HOTFIX_NAME)
        }
      }
    }
    stage('Build') {
      when {
        not { expression { skip } }
      }
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
        allOf {
          not { expression { isPr } }
          not { expression { skip } }
        }
      }
      steps {
        script {
          lastStage = env.STAGE_NAME
          if (params.RELEASE_BUILD || isHotfix) {
            env.REGISTRY_PUSH = (params.RELEASE_DRY_RUN) ? 'none' : 'all'
            env.VERSION = (isHotfix) ? env.HOTFIX_NAME : params.RELEASE_NAME
            env.DOCKER_TAG = dockerize.docker_build_specify_tag_and_push(env.VERSION, sbtbuild.getDockerPath(), sbtbuild.getDockerArtifact(), env.REGISTRY_PUSH)
          } else {
            env.REGISTRY_PUSH = 'internal'
            env.DOCKER_TAG = dockerize.docker_build('STAGING', env.GIT_COMMIT, sbtbuild.getDockerPath(), sbtbuild.getDockerArtifact(), env.REGISTRY_PUSH)
          }
          currentBuild.description = env.DOCKER_TAG
        }
      }
      post {
        success {
          script {
            boolean requiresBuild = isHotfix || params.RELEASE_BUILD
            boolean buildBypassed = isReleaseRebuild || params.RELEASE_DRY_RUN
            if (requiresBuild && !buildBypassed) {
              env.PURPOSE = (isHotfix) ? 'hotfix' : 'initial'
              env.RELEASE_ID = (isHotfix) ? env.CURRENT_RELEASE_NAME : params.RELEASE_NAME
              Map buildInfoServer = [
                "project_id": service,
                "build_id": env.DOCKER_TAG,
                "release_id": env.RELEASE_ID,
                "git_tag": env.GIT_TAG,
                "purpose": env.PURPOSE,
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
        not { expression { skip } }
        not { expression { return params.RELEASE_BUILD } }
      }
      steps {
        script {
          lastStage = env.STAGE_NAME
          env.ENVIRONMENT = (isHotfix) ? 'rc' : 'staging'
          marathonDeploy(serviceName: service, tag: env.DOCKER_TAG, environment: env.ENVIRONMENT)
        }
      }
      post {
        success {
          script {
            if (isHotfix) {
              Map deployInfo = [
                "build_id": env.DOCKER_TAG,
                "environment": env.ENVIRONMENT,
              ]
              createDeployment(
                deployInfo,
                rmsSupportedEnvironment.production
              )
            }
          }
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
