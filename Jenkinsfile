// =============================================================================
// Proxmox Backup GUI — Fast pipeline
// Trigger : GitHub webhook (every push)
// Runtime : ~60s on Jenkins LXC, no VM needed
// Tests   : Playwright against mock server — never touches prod
// =============================================================================

pipeline {
    agent any

    options {
        timeout(time: 10, unit: 'MINUTES')
        disableConcurrentBuilds()
        buildDiscarder(logRotator(numToKeepStr: '20'))
    }

    stages {
        stage('Checkout') {
            steps { checkout scm }
        }

        stage('Install deps') {
            steps {
                sh '''
                    python3 -m venv .venv
                    .venv/bin/pip install -q -r requirements.txt
                    .venv/bin/pip install -q pytest playwright
                    .venv/bin/playwright install chromium --with-deps
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
