import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from server import QuizGameManager, load_questions

class MockWebSocket:
    def __init__(self, name=""):
        self.name = name
        self.sent_messages = []

    async def send_text(self, text: str):
        self.sent_messages.append(json.loads(text))

async def test_game_logic():
    print("Iniciando teste de lógica do Quiz...")
    gm = QuizGameManager()
    assert len(gm.questions) == 5, f"Esperado 5 perguntas, encontrado {len(gm.questions)}"
    print(f"Perguntas carregadas com sucesso: {len(gm.questions)}")

    host_ws = MockWebSocket("host")
    gm.host_ws = host_ws

    # 1. Registrar 2 jogadores
    p1_ws = MockWebSocket("p1")
    p2_ws = MockWebSocket("p2")

    await gm.register_player("id_ana", "Ana Designer", p1_ws)
    await gm.register_player("id_carlos", "Carlos Grafica", p2_ws)

    assert len(gm.players) == 2
    assert gm.players["id_ana"]["name"] == "Ana Designer"
    print("Jogadores registrados no servidor!")

    # 2. Iniciar primeira questão
    await gm.start_question(0)
    assert gm.state == "QUESTION"
    assert gm.current_q_index == 0
    print("1ª Questão iniciada (Estado: QUESTION)")

    # 3. Ana responde a correta (opção 0) rapidamente
    await gm.handle_answer("id_ana", 0)
    assert gm.players["id_ana"]["is_correct"] is True
    assert gm.players["id_ana"]["score"] > 800
    print(f"Ana acertou e pontuou por velocidade: {gm.players['id_ana']['score']} pontos!")

    # 4. Carlos responde incorreta (opção 1)
    await gm.handle_answer("id_carlos", 1)
    assert gm.players["id_carlos"]["is_correct"] is False
    assert gm.players["id_carlos"]["score"] == 0
    print("Carlos errou e recebeu 0 pontos.")

    # 5. Como ambos responderam, o jogo revelou resultados
    # Aguarda pequena pausa do asyncio
    await asyncio.sleep(0.6)
    assert gm.state == "REVEAL"
    print("Resultados revelados com sucesso (Estado: REVEAL)!")

    # 6. Testar Pódio
    await gm.show_podium()
    assert gm.state == "PODIUM"
    leaderboard = gm.get_leaderboard()
    assert leaderboard[0]["name"] == "Ana Designer"
    assert leaderboard[0]["score"] > 0
    assert leaderboard[1]["name"] == "Carlos Grafica"
    print(f"Pódio validado: 1º {leaderboard[0]['name']} ({leaderboard[0]['score']} pts), 2º {leaderboard[1]['name']} ({leaderboard[1]['score']} pts)")

    # 7. Cancelar tarefas pendentes
    if gm.timer_task and not gm.timer_task.done():
        gm.timer_task.cancel()
    print("\n[SUCESSO] TODAS AS REGRAS DE NEGOCIO E PONTUACAO FORAM VALIDADAS COM SUCESSO!")

if __name__ == "__main__":
    asyncio.run(test_game_logic())
