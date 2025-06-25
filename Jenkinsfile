pipeline {
    agent {
        label 'worker'
    }
    tools {
        maven 'Maven 3'
    }

    environment {
        MAVEN_TEST_OPTIONS = ' '
        // this regex determines which branch is deployed as a snapshot
        SNAPSHOT_BRANCH_REGEX = /(^main$)/
        RELEASE_REGEX = /^([0-9]+(\.[0-9]+)*)(-(RC|beta-|alpha-)[0-9]+)?$/
        RELEASE_DEPLOY = false
        SNAPSHOT_DEPLOY = false

        DOCKER_REGISTRY = 'https://repo.heigit.org'
        DOCKER_CREDENTIALS_ID = 'docker-heigit-ci-service'
        DOCKER_REPOSITORY = 'heigit/ohsome-planet'
    }

    stages {
        stage('Build and Test') {
            steps {
                // setting up a few basic env variables like REPO_NAME and LATEST_AUTHOR
                setup_basic_env()

                mavenbuild('clean compile javadoc:jar source:jar verify -P jacoco,sign,git')
            }
            post {
                failure {
                    rocket_buildfail()
                    rocket_testfail()
                }
            }
        }

        stage('Reports and Statistics') {
            steps {
                reports_sonar_jacoco()
            }
        }

        stage('Build and Deploy Snapshot Image') {
            when {
                expression {
                    return env.BRANCH_NAME ==~ SNAPSHOT_BRANCH_REGEX && VERSION ==~ /.*-SNAPSHOT$/
                }
            }
            steps {
                script {
                    docker.withRegistry(DOCKER_REGISTRY, DOCKER_CREDENTIALS_ID) {
                        dockerImage = docker.build(DOCKER_REPOSITORY + ':' + env.BRANCH_NAME)
                        dockerImage.push()
                    }
                }
            }
            post {
                failure {
                    rocket_snapshotdeployfail()
                }
            }
        }

        stage('Build and Deploy Release Image') {
            when {
                expression {
                    return VERSION ==~ RELEASE_REGEX && env.TAG_NAME ==~ RELEASE_REGEX
                }
            }
            steps {
                script {
                    docker.withRegistry(DOCKER_REGISTRY, DOCKER_CREDENTIALS_ID) {
                        dockerImage = docker.build(DOCKER_REPOSITORY + ':' + VERSION)
                        dockerImage.push()
                        dockerImage.push('latest')
                    }
                }
            }
            post {
                failure {
                    rocket_releasedeployfail()
                }
            }
        }

        stage('Check Dependencies') {
            when {
                expression {
                    if (currentBuild.number > 1) {
                        return (((currentBuild.getStartTimeInMillis() - currentBuild.previousBuild.getStartTimeInMillis()) > 2592000000) && (env.BRANCH_NAME ==~ SNAPSHOT_BRANCH_REGEX)) //2592000000 30 days in milliseconds
                    }
                    return false
                }
            }
            steps {
                check_dependencies()
            }
        }

        stage('Wrapping Up') {
            steps {
                encourage()
                status_change()
            }
        }
    }
}
