#!/usr/bin/env groovy
// Centralize Jenkins Pipelines configuration using Shared Libraries
// https://github.com/AfterShip/deployment-jenkins-pipeline-library-ci-aftership-org
@Library("jenkins-pipeline-library@automation") _
entry {
	isDryRun = false
	flow = "java"
	ci_only = true
	configInfo = [
	      deploymentGroup          : "aftership",
	      appName                  : "seatunnel",
	      gitRepoName              : "seatunnel.git",
	      chartName                : "worker",
	      essentialDockerImage     : "java-essential",
	      essentialTag             : "openjdk-8",
	      requireStaticAsset       : false,
	      uploadAssetCredential    : "",
	      domainType               : "",
	      unitTest                 : "",
	      integrationTest          : "",
	      useEnvironmentVariable   : true,
	      hasStagingEnvironment    : true,
	      hasProductionEnvironment : true,
	      useNPM                   : false,
		    useHelmReleaseShortName  : true
	    ]
}
