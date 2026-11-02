"""
Etapa 6 - Teste de resiliencia: derruba o job do Spark no meio do processamento.

Roteiro automatico:
  1. inicia o producer em segundo plano (as reservas continuam chegando ao Kafka);
  2. apos alguns segundos, MATA o container do Spark (SIGKILL = queda abrupta, sem desligar com calma);
  3. mantem o job fora do ar por alguns segundos (as reservas se acumulam no Kafka);
  4. religa o job: ele retoma do checkpoint e processa o que ficou pendente;
  5. espera o processamento terminar e confere se nenhuma reserva foi perdida.

Uso (na raiz do projeto, com o Docker e o job `streaming` no ar; de preferencia com os topicos zerados):
    python scripts/teste_resiliencia.py
    python scripts/teste_resiliencia.py --reservas 3000 --taxa 50 --derrubar-apos 20 --pausa 15
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from verificar_consistencia import BOOTSTRAP_PADRAO, contar_mensagens, verificar  # noqa: E402

RAIZ = Path(__file__).resolve().parent.parent


def docker(*args: str) -> None:
    r = subprocess.run(["docker", "compose", *args], cwd=RAIZ, capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(f"Falha em 'docker compose {' '.join(args)}':\n{r.stderr or r.stdout}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--reservas", type=int, default=3000, help="quantas reservas enviar")
    ap.add_argument("--taxa", type=float, default=50, help="reservas por segundo")
    ap.add_argument("--derrubar-apos", type=float, default=20, help="segundos ate derrubar o Spark")
    ap.add_argument("--pausa", type=float, default=15, help="segundos com o Spark fora do ar")
    ap.add_argument("--espera-max", type=float, default=240, help="limite de espera pelo reprocessamento (s)")
    ap.add_argument("--bootstrap", default=BOOTSTRAP_PADRAO)
    args = ap.parse_args()

    base_in = contar_mensagens(args.bootstrap, "reservas")
    base_out = contar_mensagens(args.bootstrap, "previsoes")
    print(f"Estado inicial: reservas={base_in} previsoes={base_out}"
          + ("  (aviso: topicos nao estao zerados)" if base_in or base_out else ""))

    print(f"\n[1] Iniciando o producer: {args.reservas} reservas a {args.taxa:g}/s")
    producer = subprocess.Popen(
        [sys.executable, str(RAIZ / "producer" / "producer.py"), "--limite", str(args.reservas),
         "--taxa", str(args.taxa), "--bootstrap", args.bootstrap],
        cwd=RAIZ, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        time.sleep(args.derrubar_apos)
        enviadas = contar_mensagens(args.bootstrap, "reservas") - base_in
        feitas = contar_mensagens(args.bootstrap, "previsoes") - base_out
        print(f"[2] DERRUBANDO o Spark (SIGKILL). Ate agora: {enviadas} reservas enviadas, {feitas} previsoes prontas.")
        docker("kill", "-s", "SIGKILL", "streaming")

        print(f"[3] Spark fora do ar por {args.pausa:g}s; as reservas continuam chegando e ficam guardadas no Kafka...")
        time.sleep(args.pausa)
        acumuladas = contar_mensagens(args.bootstrap, "reservas") - base_in
        print(f"    reservas no Kafka: {acumuladas} | previsoes: {contar_mensagens(args.bootstrap, 'previsoes') - base_out} (paradas)")

        print("[4] Religando o Spark: ele retoma do checkpoint (a inicializacao leva ~20 s)...")
        docker("start", "streaming")
        producer.wait()
        total = args.reservas

        print("[5] Aguardando o Spark processar o que ficou pendente...")
        limite = time.time() + args.espera_max
        while time.time() < limite:
            feitas = contar_mensagens(args.bootstrap, "previsoes") - base_out
            print(f"    previsoes prontas: {feitas}/{total}", end="\r", flush=True)
            if feitas >= total:
                break
            time.sleep(3)
        print()
    finally:
        if producer.poll() is None:
            producer.terminate()

    r = verificar(args.bootstrap)
    sys.exit(0 if r["ok"] else 1)


if __name__ == "__main__":
    main()
