from .config import get_settings
from openai import OpenAI

settings = get_settings()
client = OpenAI(api_key=settings.openai_api_key)


async def generate_reply(review: dict) -> str:
    """
    review: dict con keys como reviewer_name, star_rating, comment, etc.
    """
    star = review.get("star_rating", 5)
    comment = review.get("comment", "")
    reviewer = review.get("reviewer_name", "el cliente")

    system_prompt = (
        "Eres un asistente experto en atención al cliente para pequeñas empresas. "
        "Respondes a reseñas de Google en español con un tono humano, cercano y profesional. "
        "Sé breve (3-5 frases), agradecido y, si la reseña es negativa, empático y orientado a solución. "
        "No inventes datos ni promociones agresivas."
    )

    user_prompt = f"""Reseña:
- Estrellas: {star}
- Cliente: {reviewer}
- Comentario: "{comment}"

Redacta la respuesta que pondrá el negocio en su perfil de Google.
"""

    completion = client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.5,
    )

    return completion.choices[0].message.content.strip()
