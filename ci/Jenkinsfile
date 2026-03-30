// =============================================================================
// Proxmox Backup GUI — Fast pipeline
// Trigger : GitHub webhook (every push)
// Runtime : ~60s on Jenkins LXC, no VM needed
// Tests   : Playwright against mock server — never touches prod
// =============================================================================

pipeline {
    agent any

    parameters {
        string(name: 'BRANCH', defaultValue: 'main',
               description: 'Branch to test (e.g. main, feature/backup-restore)')
    }

    options {
        timeout(time: 10, unit: 'MINUTES')
        disableConcurrentBuilds()
        buildDiscarder(logRotator(numToKeepStr: '20'))
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scmGit(
                    branches: [[name: "${params.BRANCH}"]],
                    userRemoteConfigs: scm.userRemoteConfigs
                )
                echo "Testing branch: ${params.BRANCH}"
            }
        }

        stage('Install deps') {
            steps {
                sh '''
                    python3 -m venv .venv
                    .venv/bin/pip install -q -r requirements.txt
                    .venv/bin/pip install -q pytest playwright
                    .venv/bin/playwright install chromium
                '''
            }
        }

        stage('Mock tests') {
            steps {
                sh '''
                    mkdir -p test-results
                    .venv/bin/pytest tests/test_frontend.py \
                        -v --tb=short \
                        --junit-xml=test-results/frontend-mock.xml
                '''
            }
        }
    }

    post {
        always {
            junit 'test-results/frontend-mock.xml'
            cleanWs()
        }
        success { echo "All mock tests passed — safe to merge" }
        failure { echo "Tests failed — check test-results/frontend-mock.xml" }
    }
}
