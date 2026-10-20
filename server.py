import asyncio
import json
import os
import socket
import time
import uuid
from typing import Dict, Optional

import uvicorn
from starlette.applications import Starlette
from starlette.endpoints import WebSocketEndpoint
from starlette.responses import HTMLResponse, JSONResponse, RedirectResponse
from starlette.routing import Route, WebSocketRoute, Mount
from starlette.staticfiles import StaticFiles
from starlette.websockets import WebSocket, WebSocketDisconnect

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")
QUESTIONS_FILE = os.path.join(BASE_DIR, "questions.json")

def get_local_ip() -> str:
    """Detecta o IP da máquina na rede local Wi-Fi/Ethernet."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('10.255.255.255', 1))
        ip = s.getsockname()[0]
    except Exception:
        ip = '127.0.0.1'
    finally:
        s.close()
    return ip

def load_questions():
    if os.path.exists(QUESTIONS_FILE):
        with open(QUESTIONS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

class QuizGameManager:
    def __init__(self):
        self.pin = "2026"
        self.state = "LOBBY"  # LOBBY, QUESTION, REVEAL, PODIUM
        self.questions = load_questions()
        self.current_q_index = 0
        self.q_start_time = 0.0
        self.timer_task: Optional[asyncio.Task] = None
        self.host_ws: Optional[WebSocket] = None
        self.players: Dict[str, dict] = {} # id -> {name, ws, score, streak, last_points, is_correct, answers}
        self.custom_url: Optional[str] = None

    def get_public_url(self) -> str:
        if self.custom_url:
            return self.custom_url.rstrip("/")
        ip = get_local_ip()
        return f"http://{ip}:8000"

    def reload_questions(self):
        self.questions = load_questions()

    async def broadcast_host(self, data: dict):
        if self.host_ws:
            try:
                await self.host_ws.send_text(json.dumps(data))
            except Exception:
                self.host_ws = None

    async def broadcast_players(self, data: dict):
        dead_ids = []
        msg = json.dumps(data)
        for p_id, p in self.players.items():
            if p.get("ws"):
                try:
                    await p["ws"].send_text(msg)
                except Exception:
                    dead_ids.append(p_id)
        for d in dead_ids:
            if d in self.players:
                self.players[d]["ws"] = None

    async def send_player(self, player_id: str, data: dict):
        player = self.players.get(player_id)
        if player and player.get("ws"):
            try:
                await player["ws"].send_text(json.dumps(data))
            except Exception:
                player["ws"] = None

    def get_players_summary(self):
        return [
            {"id": p_id, "name": p["name"], "score": p["score"]}
            for p_id, p in self.players.items()
            if p.get("ws") is not None
        ]

    def get_leaderboard(self):
        sorted_p = sorted(
            [p for p in self.players.values() if p.get("ws") is not None],
            key=lambda x: x["score"],
            reverse=True
        )
        return [
            {"name": p["name"], "score": p["score"], "last_points": p.get("last_points", 0)}
            for p in sorted_p
        ]

    async def register_player(self, player_id: str, name: str, ws: WebSocket):
        # Sanitizar nome
        clean_name = name.strip()[:20] or "Designer Anônimo"
        if player_id in self.players:
            self.players[player_id]["ws"] = ws
            self.players[player_id]["name"] = clean_name
        else:
            self.players[player_id] = {
                "id": player_id,
                "name": clean_name,
                "ws": ws,
                "score": 0,
                "streak": 0,
                "last_points": 0,
                "is_correct": False,
                "answers": {}
            }

        # Envia estado atual para o jogador
        await self.send_player(player_id, {
            "type": "joined_success",
            "player_id": player_id,
            "name": clean_name,
            "state": self.state,
            "current_q": self.current_q_index,
            "total_q": len(self.questions)
        })

        # Notifica host
        await self.broadcast_host({
            "type": "player_list_update",
            "count": len([p for p in self.players.values() if p.get("ws")]),
            "players": self.get_players_summary()
        })

    async def start_question(self, index: int):
        if index >= len(self.questions):
            await self.show_podium()
            return

        self.current_q_index = index
        self.state = "QUESTION"
        q = self.questions[index]
        time_limit = q.get("time_limit", 25)
        self.q_start_time = time.time()

        # Reseta respostas da rodada atual
        for p in self.players.values():
            p["is_correct"] = False
            p["last_points"] = 0

        # Dispara para o host (com todas as infos, sem mostrar a correta)
        await self.broadcast_host({
            "type": "question_start",
            "q_index": index,
            "total_q": len(self.questions),
            "theme": q.get("theme", "Produção Gráfica"),
            "question": q["question"],
            "options": q["options"],
            "time_limit": time_limit,
            "answered_count": 0,
            "total_players": len([p for p in self.players.values() if p.get("ws")])
        })

        # Dispara para todos os jogadores com as opções completas!
        await self.broadcast_players({
            "type": "question_start",
            "q_index": index,
            "total_q": len(self.questions),
            "theme": q.get("theme", "Produção Gráfica"),
            "question": q["question"],
            "options": q["options"],
            "time_limit": time_limit
        })

        # Inicia temporizador
        if self.timer_task and not self.timer_task.done():
            self.timer_task.cancel()
        self.timer_task = asyncio.create_task(self._run_timer(time_limit))

    async def _run_timer(self, total_seconds: int):
        remaining = total_seconds
        while remaining > 0 and self.state == "QUESTION":
            await self.broadcast_host({"type": "timer_tick", "remaining": remaining, "total": total_seconds})
            await self.broadcast_players({"type": "timer_tick", "remaining": remaining, "total": total_seconds})
            await asyncio.sleep(1)
            remaining -= 1

        if self.state == "QUESTION":
            await self.reveal_results()

    async def handle_answer(self, player_id: str, option_index: int):
        if self.state != "QUESTION":
            return

        player = self.players.get(player_id)
        if not player:
            return

        q_idx = self.current_q_index
        if q_idx in player["answers"]:
            return  # Já respondeu

        q = self.questions[q_idx]
        now = time.time()
        elapsed = now - self.q_start_time
        time_limit = q.get("time_limit", 25)
        remaining = max(0.0, time_limit - elapsed)

        is_correct = (option_index == q["correct"])
        points = 0
        if is_correct:
            # Fórmula: 500 base + até 500 proporcionais à agilidade
            ratio = min(1.0, remaining / time_limit)
            speed_bonus = int(500 * ratio)
            points = 500 + speed_bonus
            player["score"] += points
            player["streak"] += 1
        else:
            player["streak"] = 0

        player["is_correct"] = is_correct
        player["last_points"] = points
        player["answers"][q_idx] = {
            "option": option_index,
            "is_correct": is_correct,
            "points": points,
            "time": elapsed
        }

        # Confirma para o jogador que a resposta foi registrada
        await self.send_player(player_id, {
            "type": "answer_acknowledged",
            "option_index": option_index
        })

        # Conta quantos já responderam
        active_players = [p for p in self.players.values() if p.get("ws")]
        answered_count = sum(1 for p in active_players if q_idx in p["answers"])

        await self.broadcast_host({
            "type": "answer_progress",
            "answered_count": answered_count,
            "total_players": len(active_players)
        })

        # Se todos responderam, encerra antes do tempo!
        if answered_count >= len(active_players) and len(active_players) > 0:
            if self.timer_task and not self.timer_task.done():
                self.timer_task.cancel()
            await asyncio.sleep(0.5)
            await self.reveal_results()

    async def reveal_results(self):
        self.state = "REVEAL"
        q_idx = self.current_q_index
        q = self.questions[q_idx]
        correct_idx = q["correct"]

        # Computar estatísticas das opções escolhidas
        stats = [0] * len(q["options"])
        active_players = [p for p in self.players.values() if p.get("ws")]
        for p in active_players:
            ans = p["answers"].get(q_idx)
            if ans is not None and 0 <= ans["option"] < len(stats):
                stats[ans["option"]] += 1

        leaderboard = self.get_leaderboard()

        # Envia para o Host
        await self.broadcast_host({
            "type": "reveal_results",
            "correct_index": correct_idx,
            "explanation": q.get("explanation", ""),
            "stats": stats,
            "leaderboard": leaderboard[:5],
            "is_last_question": (q_idx + 1 >= len(self.questions))
        })

        # Envia para cada jogador com o resultado individual dele
        for p in active_players:
            ans = p["answers"].get(q_idx)
            got_it = ans["is_correct"] if ans else False
            earned = ans["points"] if ans else 0
            # Posição no ranking
            rank = next((idx + 1 for idx, item in enumerate(leaderboard) if item["name"] == p["name"]), 1)

            await self.send_player(p["id"], {
                "type": "reveal_results",
                "is_correct": got_it,
                "points_earned": earned,
                "total_score": p["score"],
                "rank": rank,
                "correct_index": correct_idx,
                "explanation": q.get("explanation", "")
            })

    async def show_podium(self):
        self.state = "PODIUM"
        leaderboard = self.get_leaderboard()
        podium = leaderboard[:3]

        await self.broadcast_host({
            "type": "show_podium",
            "podium": podium,
            "full_ranking": leaderboard
        })

        # Para os jogadores
        active_players = [p for p in self.players.values() if p.get("ws")]
        for p in active_players:
            rank = next((idx + 1 for idx, item in enumerate(leaderboard) if item["name"] == p["name"]), 1)
            await self.send_player(p["id"], {
                "type": "show_podium",
                "podium": podium,
                "my_rank": rank,
                "my_score": p["score"]
            })

    async def reset_game(self):
        self.state = "LOBBY"
        self.current_q_index = 0
        if self.timer_task and not self.timer_task.done():
            self.timer_task.cancel()
        for p in self.players.values():
            p["score"] = 0
            p["streak"] = 0
            p["last_points"] = 0
            p["is_correct"] = False
            p["answers"] = {}

        await self.broadcast_host({
            "type": "game_reset",
            "players": self.get_players_summary()
        })
        await self.broadcast_players({
            "type": "game_reset"
        })

game = QuizGameManager()

# --- HTTP Endpoints ---

async def index_view(request):
    return RedirectResponse(url="/player")

async def player_view(request):
    html_file = os.path.join(STATIC_DIR, "player.html")
    if os.path.exists(html_file):
        with open(html_file, "r", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    return HTMLResponse("<h1>Página do Jogador não encontrada</h1>", status_code=404)

async def host_view(request):
    html_file = os.path.join(STATIC_DIR, "host.html")
    if os.path.exists(html_file):
        with open(html_file, "r", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    return HTMLResponse("<h1>Página do Telão não encontrada</h1>", status_code=404)

async def api_info(request):
    host_header = request.headers.get("host")
    proto = request.headers.get("x-forwarded-proto", request.url.scheme)
    if host_header and not host_header.startswith("localhost") and not host_header.startswith("127.0.0.1"):
        url = f"{proto}://{host_header}"
    else:
        url = game.get_public_url()

    return JSONResponse({
        "local_ip": get_local_ip(),
        "port": 8000,
        "join_url": url,
        "pin": game.pin,
        "players_count": len([p for p in game.players.values() if p.get("ws")]),
        "state": game.state,
        "total_questions": len(game.questions)
    })

async def api_set_url(request):
    try:
        data = await request.json()
        new_url = data.get("url")
        if new_url:
            game.custom_url = new_url
            return JSONResponse({"status": "ok", "url": game.get_public_url()})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    return JSONResponse({"error": "URL inválida"}, status_code=400)

# --- WebSockets ---

async def ws_host(websocket: WebSocket):
    await websocket.accept()
    game.host_ws = websocket
    
    # Determina URL pública com base no cabeçalho
    host_header = websocket.headers.get("host")
    proto = websocket.headers.get("x-forwarded-proto", "http")
    if host_header and not host_header.startswith("localhost") and not host_header.startswith("127.0.0.1"):
        detected_url = f"{proto}://{host_header}"
    else:
        detected_url = game.get_public_url()

    await websocket.send_text(json.dumps({
        "type": "host_init",
        "pin": game.pin,
        "state": game.state,
        "local_ip": get_local_ip(),
        "join_url": detected_url,
        "total_questions": len(game.questions),
        "players": game.get_players_summary()
    }))

    try:
        while True:
            raw = await websocket.receive_text()
            data = json.loads(raw)
            action = data.get("action")

            if action == "start_quiz":
                await game.start_question(0)
            elif action == "next_question":
                await game.start_question(game.current_q_index + 1)
            elif action == "force_end_timer":
                if game.state == "QUESTION":
                    if game.timer_task and not game.timer_task.done():
                        game.timer_task.cancel()
                    await game.reveal_results()
            elif action == "show_podium":
                await game.show_podium()
            elif action == "reset_game":
                await game.reset_game()
            elif action == "set_custom_url":
                game.custom_url = data.get("url", "").strip() or None
                await websocket.send_text(json.dumps({
                    "type": "url_updated",
                    "join_url": game.get_public_url()
                }))
    except WebSocketDisconnect:
        game.host_ws = None
    except Exception:
        game.host_ws = None

async def ws_player(websocket: WebSocket):
    await websocket.accept()
    player_id = None
    try:
        while True:
            raw = await websocket.receive_text()
            data = json.loads(raw)
            action = data.get("action")

            if action == "join":
                player_id = data.get("player_id") or str(uuid.uuid4())[:8]
                name = data.get("name") or "Colega Designer"
                await game.register_player(player_id, name, websocket)

            elif action == "answer":
                if player_id:
                    option_idx = data.get("option_index")
                    if option_idx is not None:
                        await game.handle_answer(player_id, int(option_idx))
    except WebSocketDisconnect:
        if player_id and player_id in game.players:
            game.players[player_id]["ws"] = None
            await game.broadcast_host({
                "type": "player_list_update",
                "count": len([p for p in game.players.values() if p.get("ws")]),
                "players": game.get_players_summary()
            })
    except Exception:
        pass

routes = [
    Route("/", endpoint=index_view),
    Route("/player", endpoint=player_view),
    Route("/host", endpoint=host_view),
    Route("/api/info", endpoint=api_info),
    Route("/api/set_url", endpoint=api_set_url, methods=["POST"]),
    WebSocketRoute("/ws/host", endpoint=ws_host),
    WebSocketRoute("/ws/player", endpoint=ws_player),
    Mount("/static", app=StaticFiles(directory=STATIC_DIR), name="static")
]

os.makedirs(STATIC_DIR, exist_ok=True)
app = Starlette(routes=routes)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    local_ip = get_local_ip()
    print("=" * 65)
    print("  QUIZ DE FIXACAO: SERIGRAFIA & ROTOGRAVURA")
    print("=" * 65)
    print(f" > Telao do Apresentador:  http://localhost:{port}/host")
    print(f" > Celular dos Jogadores:  http://{local_ip}:{port}")
    print("=" * 65)
    print("Pressione Ctrl+C para encerrar o servidor.")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")
