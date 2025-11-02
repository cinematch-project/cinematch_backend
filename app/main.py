from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes import router as api_router
from app.tasks.on_startup_create_recommender import on_startup_create_recommender
from app.tasks.on_startup_populate_db import on_startup_populate_db

app = FastAPI(title="CineMatch API")

origins = [
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


def startup():
    engine = on_startup_populate_db()
    app.state.recommender = on_startup_create_recommender(engine)


app.add_event_handler("startup", startup)

app.get("/health")(lambda: {"status": "ok"})
app.include_router(api_router, prefix="/api")
