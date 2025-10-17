// Jenkinsfile
pipeline {
    agent any
    
    environment {
        DOCKER_IMAGE = 'email-scraper'
        DOCKER_TAG = "${BUILD_NUMBER}"
        GITHUB_REPO = 'your-username/email-scraper'
    }
    
    stages {
        stage('Checkout') {
            steps {
                git branch: 'main',
                    url: "https://github.com/${GITHUB_REPO}.git",
                    credentialsId: 'github-credentials'
            }
        }
        
        stage('Build Docker Image') {
            steps {
                script {
                    docker.build("${DOCKER_IMAGE}:${DOCKER_TAG}")
                    docker.build("${DOCKER_IMAGE}:latest")
                }
            }
        }
        
        stage('Test') {
            steps {
                script {
                    docker.image("${DOCKER_IMAGE}:${DOCKER_TAG}").inside {
                        sh 'python -m pytest tests/ || true'
                    }
                }
            }
        }
        
        stage('Deploy') {
            steps {
                script {
                    sh '''
                        docker-compose down || true
                        docker-compose up -d
                    '''
                }
            }
        }
        
        stage('Health Check') {
            steps {
                script {
                    sleep(time: 10, unit: 'SECONDS')
                    sh 'curl -f http://localhost:8501/_stcore/health || exit 1'
                }
            }
        }
    }
    
    post {
        success {
            echo 'Deployment successful!'
        }
        failure {
            echo 'Deployment failed!'
            sh 'docker-compose logs'
        }
    }
}
