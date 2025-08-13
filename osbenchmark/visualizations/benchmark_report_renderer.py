
def render_results_html(test_run, cfg) -> str:
    """
    Build a “lighter” OpenSearch-themed HTML report for the given TestRun.
    """
    # 1) Normalize the test_run to a dict
    if isinstance(test_run, dict):
        doc = test_run
    elif hasattr(test_run, "as_dict"):
        doc = test_run.as_dict()
    else:
        # fallback minimal dict
        doc = {
            "test-run-id": test_run.test_run_id,
            "benchmark-version": test_run.benchmark_version,
            "benchmark-revision": test_run.benchmark_revision,
            "environment": test_run.environment_name,
            "pipeline": test_run.pipeline,
            "workload": test_run.workload_name,
            "test-procedure": getattr(test_run, "test_procedure_name", None),
            "cluster": {
                "revision": test_run.revision,
                "distribution-version": test_run.distribution_version,
                "distribution-flavor": test_run.distribution_flavor,
                "provision-config-revision": test_run.provision_config_revision,
            }
        }
        if getattr(test_run, "results", None):
            # results might already be a dict or an object
            if isinstance(test_run.results, dict):
                doc["results"] = test_run.results
            elif hasattr(test_run.results, "as_dict"):
                doc["results"] = test_run.results.as_dict()

    # 2) Pull top-level fields
    test_id       = doc.get("test-run-id", "<unknown>")
    osb_ver       = doc.get("benchmark-version", "")
    osb_rev       = doc.get("benchmark-revision", "")
    environment   = doc.get("environment", "")
    pipeline      = doc.get("pipeline", "")
    workload      = doc.get("workload", "")
    test_procedure= doc.get("test-procedure", "")

    # 3) Cluster info
    cluster_info    = doc.get("cluster", {})
    distro_ver      = cluster_info.get("distribution-version", "")
    distro_flav     = cluster_info.get("distribution-flavor", "")
    prov_conf_rev   = cluster_info.get("provision-config-revision", None)

    # 4) Config table dict
    config_dict = {
        "OSB Version":               osb_ver,
        "OSB Revision (git)":        osb_rev,
        "Environment":               environment,
        "Pipeline":                  pipeline,
        "Workload":                  workload,
        "Test Procedure":            test_procedure,
        "Distribution Version":      distro_ver,
        "Distribution Flavor":       distro_flav,
        "Provision Config Revision": prov_conf_rev,
    }

    # 5) Extract op_metrics
    results_dict = doc.get("results", {}) or {}
    op_metrics   = results_dict.get("op_metrics", [])

    # Build rows
    table_rows = []
    for item in op_metrics:
        th = item.get("throughput", {})
        st = item.get("service_time", {})
        clients = item.get("search_clients") or item.get("clients", "") or "–"
        table_rows.append({
            "task":              item.get("task", ""),
            "operation":         item.get("operation", ""),
            "throughput_mean":   th.get("mean"),
            "throughput_unit":   th.get("unit", ""),
            "service_time_mean": st.get("mean"),
            "service_time_unit": st.get("unit", ""),
            "search_clients":    clients,
        })

    # 6) Render helpers
    def render_config_table(cfg_d):
        rows = ""
        for k, v in cfg_d.items():
            disp = "N/A" if v is None else v
            rows += f"<tr><th>{k}</th><td>{disp}</td></tr>"
        return f"<table class='config-table'><tbody>{rows}</tbody></table>"

    def render_metrics_table(rows):
        header = (
            "<tr>"
            "<th>Task</th><th>Operation</th>"
            "<th>Throughput (mean)</th>"
            "<th>Service Time (mean)</th>"
            "<th>Search Clients</th>"
            "</tr>"
        )
        body = ""
        for r in rows:
            th_val = f"{r['throughput_mean']} {r['throughput_unit']}" if r['throughput_mean'] is not None else "–"
            st_val = f"{r['service_time_mean']} {r['service_time_unit']}" if r['service_time_mean'] is not None else "–"
            body += (
                "<tr>"
                f"<td>{r['task']}</td>"
                f"<td>{r['operation']}</td>"
                f"<td>{th_val}</td>"
                f"<td>{st_val}</td>"
                f"<td>{r['search_clients']}</td>"
                "</tr>"
            )
        return f"<table class='metrics-table'><thead>{header}</thead><tbody>{body}</tbody></table>"

    # 7) Put it all together
    cfg_table_html = render_config_table(config_dict)
    metrics_html   = render_metrics_table(table_rows)

    return f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
          <meta charset="utf-8" />
          <title>OpenSearch Benchmark Report &mdash; {test_id}</title>
          <style>
            /* ───────────────────────────────────────────────────────────────── */
            /* Base Styles + Resets */
            body {{
              margin: 0;
              padding: 0;
              font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Oxygen,
                           Ubuntu, Cantarell, "Open Sans", "Helvetica Neue", sans-serif;
              background-color: #f5f7fa;
              color: #333;
            }}
            a {{ text-decoration: none; color: inherit; }}

            /* Header Bar (light‐blue version) */
            header {{
              background-color: #2374aa;
              color: white;
              padding: 1rem 2rem;
              display: flex;
              align-items: center;
            }}
            header h1 {{
              margin: 0;
              font-size: 1.5rem;
              font-weight: 400;
              color: #e1f0ff; /* a very light, almost‐white blue */
            }}

            /* Container to center the content */
            .container {{
              max-width: 1100px;
              margin: 2rem auto;
              padding: 0 1rem;
            }}

            /* Card Styles */
            .card {{
              background-color: white;
              border-radius: 8px;
              box-shadow: 0 2px 4px rgba(0, 0, 0, 0.08);
              margin-bottom: 2rem;
              padding: 1.5rem 2rem;
            }}
            .card h2 {{
              margin-top: 0;
              margin-bottom: 0.75rem;
              color: #2374aa;
              font-size: 1.35rem;
              font-weight: 500;
            }}
            .subtitle {{
              margin-top: 0.25rem;
              margin-bottom: 1rem;
              font-size: 0.95rem;
              color: #555;
            }}

            /* Configuration Table (key/value pairs) */
            table.config-table {{
              width: 100%;
              border-collapse: collapse;
            }}
            table.config-table th,
            table.config-table td {{
              text-align: left;
              padding: 0.5rem 0.75rem;
              border-bottom: 1px solid #e0e0e0;
            }}
            table.config-table th {{
              width: 30%;
              font-weight: 500;
              color: #444;
              background-color: #f0f4f8;
            }}

            /* Metrics Table (per‐operation) */
            table.metrics-table {{
              width: 100%;
              border-collapse: collapse;
              margin-top: 1rem;
            }}
            table.metrics-table th,
            table.metrics-table td {{
              border: 1px solid #ddd;
              padding: 0.5rem 0.75rem;
              font-size: 0.9rem;
            }}
            table.metrics-table th {{
              background-color: #e8f2fa;
              color: #2374aa;
              font-weight: 500;
              text-align: left;
            }}
            table.metrics-table tr:nth-child(even) {{
              background-color: #fbfcfe;
            }}

            /* Responsive tweaks */
            @media (max-width: 800px) {{
              header {{
                flex-direction: column;
                align-items: flex-start;
              }}
              header h1 {{ font-size: 1.25rem; }}
              .container {{
                margin: 1rem auto;
                padding: 0 0.5rem;
              }}
              table.metrics-table th,
              table.metrics-table td {{
                font-size: 0.8rem;
                padding: 0.4rem 0.5rem;
              }}
            }}
            /* ───────────────────────────────────────────────────────────────── */
          </style>
        </head>
        <body>
          <!-- ─── Header Bar ───────────────────────────────────────────────── -->
          <header>
            <h1>OpenSearch Benchmark Report: <code>{test_id}</code></h1>
          </header>

          <!-- ─── Main Content ──────────────────────────────────────────────── -->
          <div class="container">
            <!-- Configuration Card -->
            <div class="card">
              <h2>Cluster & Benchmark Configuration</h2>
              <div class="subtitle">
                OSB version, Git revision, environment, pipeline, workload, distro info, etc.
              </div>
              {cfg_table_html}
            </div>

            <!-- Metrics Card -->
            <div class="card">
              <h2>Redline Results (Per Task / Operation)</h2>
              <div class="subtitle">
                Throughput &amp; Service Time (mean), plus any “search clients” used.
              </div>
              {metrics_html}
            </div>
          </div>
        </body>
        </html>
        """
