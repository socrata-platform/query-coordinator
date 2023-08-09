@Library('socrata-pipeline-library')

def service = 'query-coordinator'
def project_wd = service
def isPr = env.CHANGE_ID != null;

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
    SERVICE = service
  }
  stages {
    stage('Release Tag') {
      when {
        expression { return params.RELEASE_BUILD }
      }
      steps {
        script {
          if (params.RELEASE_DRY_RUN) {
            echo 'DRY RUN: Skipping release tag creation'
          }
          else {
            releaseTag.create(params.RELEASE_NAME)
          }
        }
      }
    }
    stage('Build') {
      steps {
        script {
          sbtbuild.setScalaVersion("2.12")
          sbtbuild.setSubprojectName("queryCoordinator")
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
          if (params.RELEASE_BUILD) {
            env.REGISTRY_PUSH = (params.RELEASE_DRY_RUN) ? 'none' : 'all'
            env.DOCKER_TAG = dockerize.docker_build_specify_tag_and_push(params.RELEASE_NAME, sbtbuild.getDockerPath(), sbtbuild.getDockerArtifact(), env.REGISTRY_PUSH)
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
            if (params.RELEASE_BUILD && !params.RELEASE_DRY_RUN){
              echo env.DOCKER_TAG // For now, just print the deploy tag in the console output -- later, communicate to release metadata service
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
          // uses env.SERVICE and env.DOCKER_TAG, deploys to staging by default
          marathonDeploy()
        }
      }
    }
  }
}
