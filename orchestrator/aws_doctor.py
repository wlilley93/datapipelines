from __future__ import annotations

import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import questionary
from rich.console import Console
from rich.panel import Panel

console = Console()


def _read_simple_env(path: Path) -> Dict[str, str]:
    out: Dict[str, str] = {}
    try:
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip("'").strip('"')
            if k and k not in out:
                out[k] = v
    except Exception:
        pass
    return out


@dataclass(frozen=True)
class AwsCtx:
    region: Optional[str]
    profile: Optional[str]


def _aws_available() -> bool:
    return shutil.which("aws") is not None


def _aws_base(ctx: AwsCtx) -> List[str]:
    cmd = ["aws"]
    if ctx.profile:
        cmd += ["--profile", ctx.profile]
    if ctx.region:
        cmd += ["--region", ctx.region]
    return cmd


def _aws_json(ctx: AwsCtx, args: List[str]) -> Any:
    env = {**os.environ, "AWS_PAGER": ""}
    res = subprocess.run(_aws_base(ctx) + args + ["--output", "json"], capture_output=True, text=True, env=env)
    if res.returncode != 0:
        msg = (res.stderr or res.stdout or "").strip()
        raise RuntimeError(msg or f"aws failed: {' '.join(args)}")
    try:
        return json.loads(res.stdout or "null")
    except Exception as e:
        raise RuntimeError(f"Failed to parse aws output as JSON: {e}")


def _looks_like_access_denied(msg: str) -> bool:
    m = (msg or "").lower()
    return (
        "unauthorizedoperation" in m
        or "accessdenied" in m
        or "not authorized" in m
        or "is not authorized" in m
        or "access denied" in m
    )


def _policy_hint() -> str:
    # Minimal set for what this doctor attempts.
    return (
        "{\n"
        '  "Version": "2012-10-17",\n'
        '  "Statement": [\n'
        "    {\n"
        '      "Effect": "Allow",\n'
        '      "Action": [\n'
        '        "ec2:DescribeInstances",\n'
        '        "ec2:DescribeSecurityGroups",\n'
        '        "ec2:AuthorizeSecurityGroupIngress",\n'
        '        "rds:DescribeDBInstances"\n'
        "      ],\n"
        '      "Resource": "*"\n'
        "    }\n"
        "  ]\n"
        "}\n"
    )


def _prompt_sg_id(label: str) -> Optional[str]:
    value = questionary.text(f"Enter {label} security group id (sg-...):").ask()
    if not value:
        return None
    value = value.strip()
    if not value.startswith("sg-"):
        return None
    return value


def _prompt_sg_ids(label: str) -> List[str]:
    raw = questionary.text(
        f"Enter {label} security group id(s), comma-separated (sg-...):"
    ).ask()
    if not raw:
        return []
    parts = [p.strip() for p in raw.split(",")]
    out = [p for p in parts if p.startswith("sg-")]
    dedup: List[str] = []
    for x in out:
        if x not in dedup:
            dedup.append(x)
    return dedup


def _get_instance_sg_ids(ctx: AwsCtx, instance_id: str) -> List[str]:
    payload = _aws_json(ctx, ["ec2", "describe-instances", "--instance-ids", instance_id])
    sgs: List[str] = []
    for r in (payload or {}).get("Reservations", []) or []:
        for inst in (r or {}).get("Instances", []) or []:
            for sg in (inst or {}).get("SecurityGroups", []) or []:
                gid = (sg or {}).get("GroupId")
                if isinstance(gid, str) and gid:
                    sgs.append(gid)
    # de-dupe preserve order
    out: List[str] = []
    for x in sgs:
        if x not in out:
            out.append(x)
    return out


def _get_rds_sg_ids_by_endpoint(ctx: AwsCtx, endpoint: str) -> Tuple[Optional[str], List[str]]:
    payload = _aws_json(ctx, ["rds", "describe-db-instances"])
    db_id: Optional[str] = None
    sg_ids: List[str] = []

    for db in (payload or {}).get("DBInstances", []) or []:
        ep = ((db or {}).get("Endpoint") or {}).get("Address")
        if ep != endpoint:
            continue
        db_id = (db or {}).get("DBInstanceIdentifier")
        for sg in (db or {}).get("VpcSecurityGroups", []) or []:
            gid = (sg or {}).get("VpcSecurityGroupId")
            if isinstance(gid, str) and gid:
                sg_ids.append(gid)
        break

    # de-dupe preserve order
    out: List[str] = []
    for x in sg_ids:
        if x not in out:
            out.append(x)
    return db_id, out


def _sg_allows_from_group(sg_desc: Dict[str, Any], source_sg_id: str, port: int) -> bool:
    for perm in (sg_desc.get("IpPermissions") or []) if isinstance(sg_desc, dict) else []:
        if not isinstance(perm, dict):
            continue
        ip_proto = perm.get("IpProtocol")
        from_port = perm.get("FromPort")
        to_port = perm.get("ToPort")
        if ip_proto not in ("tcp", "-1"):
            continue
        if from_port is not None and int(from_port) > int(port):
            continue
        if to_port is not None and int(to_port) < int(port):
            continue
        pairs = perm.get("UserIdGroupPairs") or []
        for p in pairs:
            if isinstance(p, dict) and p.get("GroupId") == source_sg_id:
                return True
    return False


def _describe_sg(ctx: AwsCtx, sg_id: str) -> Dict[str, Any]:
    payload = _aws_json(ctx, ["ec2", "describe-security-groups", "--group-ids", sg_id])
    sgs = (payload or {}).get("SecurityGroups") or []
    if not sgs:
        return {}
    if isinstance(sgs[0], dict):
        return sgs[0]
    return {}


def _authorize_ingress(ctx: AwsCtx, rds_sg_id: str, source_sg_id: str, port: int) -> str:
    env = {**os.environ, "AWS_PAGER": ""}
    cmd = _aws_base(ctx) + [
        "ec2",
        "authorize-security-group-ingress",
        "--group-id",
        rds_sg_id,
        "--protocol",
        "tcp",
        "--port",
        str(int(port)),
        "--source-group",
        source_sg_id,
    ]
    res = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if res.returncode != 0:
        msg = (res.stderr or res.stdout or "").strip()
        return msg or "authorize-security-group-ingress failed"
    return "ok"


def aws_tunnel_doctor() -> None:
    """
    Diagnoses (and optionally fixes) the most common reason SSM port-forward hangs:
    the SSM target instance cannot reach the RDS endpoint on 5432 because the RDS SG
    does not allow inbound from the instance SG.
    """
    console.clear()
    console.print(Panel("[bold cyan]AWS Tunnel Doctor[/bold cyan]"))

    if not _aws_available():
        console.print(Panel("[red]AWS CLI not found[/red]\nInstall `awscli` first.", title="Missing Dependency"))
        input("\nPress Enter...")
        return

    env_path = Path("infrastructure/.env.aws")
    env = _read_simple_env(env_path)

    region = env.get("AWS_REGION") or os.getenv("AWS_REGION")
    profile = env.get("AWS_PROFILE") or os.getenv("AWS_PROFILE")
    ctx = AwsCtx(region=region, profile=profile)

    ssm_target = env.get("SSM_TARGET") or os.getenv("SSM_TARGET")
    rds_host = env.get("SSM_HOST") or os.getenv("SSM_HOST")
    port = int(env.get("SSM_REMOTE_PORT") or os.getenv("SSM_REMOTE_PORT") or "5432")

    if not ssm_target or not rds_host:
        console.print(
            Panel(
                "[red]Missing config[/red]\n"
                "Set `SSM_TARGET` and `SSM_HOST` in `infrastructure/.env.aws` (or your environment).",
                title="Config Error",
            )
        )
        input("\nPress Enter...")
        return

    console.print(f"[dim]Using profile={profile or '(default)'} region={region or '(default)'}[/dim]")
    console.print(f"[dim]SSM target: {ssm_target}[/dim]")
    console.print(f"[dim]RDS endpoint: {rds_host}:{port}[/dim]")

    try:
        try:
            inst_sgs = _get_instance_sg_ids(ctx, ssm_target)
        except Exception as e:
            msg = str(e)
            if _looks_like_access_denied(msg):
                console.print(
                    Panel(
                        "[yellow]Limited AWS permissions detected[/yellow]\n"
                        "This profile cannot call `ec2:DescribeInstances`, so the doctor cannot automatically discover the EC2 security group.\n\n"
                        "Workarounds:\n"
                        "- Switch to an AWS profile/role with the required permissions, or\n"
                        "- Paste the EC2 security group id manually (sg-...).\n\n"
                        "Minimal IAM policy:\n"
                        f"[dim]{_policy_hint()}[/dim]",
                        title="Access Denied",
                    )
                )
                manual = _prompt_sg_id("EC2 (SSM target)")
                if not manual:
                    input("\nPress Enter...")
                    return
                inst_sgs = [manual]
            else:
                raise

        if not inst_sgs:
            raise RuntimeError("Could not determine the EC2 security groups for the SSM target.")
        source_sg = inst_sgs[0]
        if len(inst_sgs) > 1:
            choice = questionary.select(
                "Which EC2 security group should be allowed to access RDS?",
                choices=inst_sgs,
                default=inst_sgs[0],
            ).ask()
            if choice:
                source_sg = str(choice)

        try:
            db_id, rds_sgs = _get_rds_sg_ids_by_endpoint(ctx, rds_host)
        except Exception as e:
            msg = str(e)
            if _looks_like_access_denied(msg):
                console.print(
                    Panel(
                        "[yellow]Limited AWS permissions detected[/yellow]\n"
                        "This profile cannot call `rds:DescribeDBInstances`, so the doctor cannot automatically discover the RDS security group.\n\n"
                        "Workarounds:\n"
                        "- Switch to an AWS profile/role with the required permissions, or\n"
                        "- Paste the RDS security group id(s) manually (sg-...).\n\n"
                        "Minimal IAM policy:\n"
                        f"[dim]{_policy_hint()}[/dim]",
                        title="Access Denied",
                    )
                )
                rds_sgs = _prompt_sg_ids("RDS")
                db_id = None
            else:
                raise

        if not rds_sgs:
            console.print(
                Panel(
                    "[red]Could not determine RDS security groups[/red]\n"
                    "Provide the RDS SG id(s) manually or run with a profile that can call `rds:DescribeDBInstances`.",
                    title="Missing Data",
                )
            )
            input("\nPress Enter...")
            return

        console.print(
            Panel(
                f"EC2 SG: {source_sg}\nRDS SGs: {', '.join(rds_sgs)}" + (f"\nDB: {db_id}" if db_id else ""),
                title="Detected",
            )
        )

        missing: List[str] = []
        for rds_sg in rds_sgs:
            try:
                desc = _describe_sg(ctx, rds_sg)
                if not _sg_allows_from_group(desc, source_sg, port):
                    missing.append(rds_sg)
            except Exception as e:
                msg = str(e)
                if _looks_like_access_denied(msg):
                    # If we can't describe SGs, we can't prove the rule exists; assume it's missing.
                    missing.append(rds_sg)
                else:
                    raise

        if not missing:
            console.print(Panel("[green]✅ RDS security group already allows inbound from the EC2 SG[/green]", title="OK"))
            input("\nPress Enter...")
            return

        console.print(
            Panel(
                "[yellow]Missing inbound rule[/yellow]\n"
                f"RDS SG(s) do not allow tcp/{port} from {source_sg}:\n"
                + "\n".join(f"- {sg}" for sg in missing),
                title="Needs Fix",
            )
        )

        if not questionary.confirm("Add the inbound rule automatically now?", default=False).ask():
            example = (
                f"aws ec2 authorize-security-group-ingress --group-id <RDS_SG> "
                f"--protocol tcp --port {port} --source-group {source_sg}"
            )
            console.print(Panel(example, title="Manual Command"))
            input("\nPress Enter...")
            return

        results: List[str] = []
        for rds_sg in missing:
            msg = _authorize_ingress(ctx, rds_sg, source_sg, port)
            results.append(f"{rds_sg}: {msg}")
        console.print(Panel("\n".join(results), title="Applied"))

        console.print(
            Panel(
                "Next: rerun Schema Sync. If the tunnel still times out, routing/NACL/DNS may be the issue.",
                title="Next",
            )
        )

    except Exception as e:
        msg = str(e)
        if _looks_like_access_denied(msg):
            console.print(
                Panel(
                    "[red]Doctor failed due to AWS permissions[/red]\n"
                    f"{msg}\n\n"
                    "Fix: run with a profile/role that has at least:\n"
                    "- ec2:DescribeInstances\n"
                    "- ec2:DescribeSecurityGroups\n"
                    "- ec2:AuthorizeSecurityGroupIngress\n"
                    "- rds:DescribeDBInstances\n\n"
                    f"Minimal IAM policy:\n[dim]{_policy_hint()}[/dim]",
                    title="Access Denied",
                )
            )
        else:
            console.print(Panel(f"[red]Doctor failed[/red]\n{msg}", title="Error"))

    input("\nPress Enter...")
