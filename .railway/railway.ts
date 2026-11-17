import { defineRailway, github, postgres, project, service, volume } from "railway/iac";

export default defineRailway(() => {
  const dealflowAi = github("Giss07/dealflow-ai", { branch: "main", checkSuites: false });

  const Postgres = postgres("Postgres", { region: "us-west2" });
  Postgres.deploy = { multiRegionConfig: { "us-west2": { numReplicas: 1 } } };
  Postgres.networking = { privateNetworkEndpoint: "postgres" };
  const postgresVolume = volume("postgres-volume", { alerts: { usage: { "100": {}, "80": {}, "95": {} } }, allowOnlineResize: true, region: "us-west2", sizeMB: 500 });
  const dealflowAi2 = service("dealflow-ai", {
    source: dealflowAi,
    build: { buildEnvironment: "V3", builder: "DOCKERFILE", dockerfilePath: "Dockerfile" },
    start: "sh -c 'if [ \"$SERVICE_TYPE\" = \"worker\" ]; then python worker.py; elif [ \"$SERVICE_TYPE\" = \"mcp\" ]; then python mcp_server.py; else gunicorn app:app --bind 0.0.0.0:${PORT:-8080} --workers 2; fi'",
    healthcheck: "/health",
    replicas: { "us-west2": 1 },
    deploy: { restartPolicyType: "ALWAYS" },
  });
  const web = service("web", {
    source: dealflowAi,
    build: { buildEnvironment: "V3", builder: "DOCKERFILE", dockerfilePath: "Dockerfile" },
    start: "sh -c 'if [ \"$SERVICE_TYPE\" = \"worker\" ]; then python worker.py; elif [ \"$SERVICE_TYPE\" = \"mcp\" ]; then python mcp_server.py; else gunicorn app:app --bind 0.0.0.0:${PORT:-8080} --workers 2; fi'",
    healthcheck: "/health",
    replicas: { "us-west2": 1 },
    deploy: { restartPolicyType: "ALWAYS" },
  });
  const worker = service("worker", {
    source: dealflowAi,
    build: { buildEnvironment: "V3", builder: "DOCKERFILE", dockerfilePath: "Dockerfile" },
    start: "sh -c 'if [ \"$SERVICE_TYPE\" = \"worker\" ]; then python worker.py; elif [ \"$SERVICE_TYPE\" = \"mcp\" ]; then python mcp_server.py; else gunicorn app:app --bind 0.0.0.0:${PORT:-8080} --workers 2; fi'",
    healthcheck: "/health",
    replicas: { "us-west2": 1 },
    deploy: { restartPolicyType: "ALWAYS" },
  });
  const MCP = service("MCP", {
    source: dealflowAi,
    build: { buildEnvironment: "V3", builder: "DOCKERFILE", dockerfilePath: "Dockerfile" },
    start: "sh -c 'if [ \"$SERVICE_TYPE\" = \"worker\" ]; then python worker.py; elif [ \"$SERVICE_TYPE\" = \"mcp\" ]; then python mcp_server.py; else gunicorn app:app --bind 0.0.0.0:${PORT:-8080} --workers 2; fi'",
    healthcheck: "/health",
    replicas: { "us-west2": 1 },
    deploy: { restartPolicyType: "ALWAYS" },
    networking: { privateNetworkEndpoint: "mcp" },
  });

  return project("ingenious-stillness", {
    variables: { managed: false },
    resources: [dealflowAi2, web, worker, MCP, Postgres, postgresVolume],
  });
});
