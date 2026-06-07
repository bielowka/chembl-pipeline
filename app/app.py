"""Streamlit AI Drug Discovery Agent.

Uzywa HybridGINE (best_model_pure.pth) wytrenowanego skryptem training/train_pure.py
na zbiorze all_meta.parquet wygenerowanym przez DAG `chembl_processing_pipeline_with_datasets`
(preprocessing/data_platform/dags/afdatasets_dag.py + pipeline.py).

LLM: lokalna Ollama przez endpoint kompatybilny z OpenAI SDK
(http://localhost:11434/v1). Model domyslny: llama3.1 (musi byc dostepny w `ollama list`).
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import streamlit as st
import torch
from openai import OpenAI

from rdkit.Chem import Draw

from model import HybridGINE
from utils import (
    ALLOWED_BAO_FORMATS,
    ALLOWED_ORGANISMS,
    ALLOWED_STANDARD_TYPES,
    DEFAULT_BAO_FORMAT,
    DEFAULT_ORGANISM,
    DEFAULT_STANDARD_TYPE,
    NUM_EDGE_FEATURES,
    NUM_NODE_FEATURES,
    NUM_TABULAR_FEATURES,
    chem_properties_report,
    predict_pic50,
    smiles_to_image,
)

st.set_page_config(page_title="AI Drug Discovery Agent", page_icon="🧪", layout="wide")

APP_DIR = Path(__file__).resolve().parent
MODEL_PATH = APP_DIR / "model.pth"

LLM_BASE_URL = "http://localhost:11434/v1"
LLM_API_KEY_DEFAULT = "ollama"
LLM_MODEL = "llama3.1"

if not MODEL_PATH.exists():
    st.error(f"Krytyczny błąd: Nie znaleziono modelu w {MODEL_PATH}")
    st.stop()


def get_device() -> torch.device:
    if torch.backends.mps.is_available():
        return torch.device("mps")
    if torch.cuda.is_available():
        return torch.device("cuda")
    return torch.device("cpu")


device = get_device()


@st.cache_resource(show_spinner="Wczytywanie wag HybridGINE...")
def load_trained_model() -> HybridGINE:
    model = HybridGINE(
        num_node_features=NUM_NODE_FEATURES,
        num_edge_features=NUM_EDGE_FEATURES,
        num_tabular_features=NUM_TABULAR_FEATURES,
    )
    state = torch.load(MODEL_PATH, map_location=device)
    model.load_state_dict(state)
    model.to(device)
    model.eval()
    return model


def evaluate_pic50(
    smiles: str,
    standard_type: str = DEFAULT_STANDARD_TYPE,
    bao_format: str = DEFAULT_BAO_FORMAT,
    organism: str = DEFAULT_ORGANISM,
) -> dict:
    model = load_trained_model()
    try:
        value = predict_pic50(model, smiles, device, standard_type, bao_format, organism)
    except Exception as exc:
        return {"error": f"Predykcja nie powiodla sie: {exc}"}

    if value is None:
        return {"error": f"Niepoprawny SMILES: {smiles!r}"}

    ic50_nM = (10 ** (-value)) * 1e9
    return {
        "smiles": smiles,
        "pIC50": round(value, 3),
        "estimated_IC50_nM": round(ic50_nM, 2),
        "context": {
            "standard_type": standard_type,
            "bao_format": bao_format,
            "organism": organism,
        },
    }


def render_molecule_image(smiles: str) -> None:
    """Rysuje strukture czasteczki w aktualnym kontekscie Streamlit (np. chat_message)."""
    img = smiles_to_image(smiles)
    if img is not None:
        st.image(img, caption=f"Struktura czasteczki: {smiles}")


def get_chem_properties(smiles: str) -> dict:
    report = chem_properties_report(smiles)
    if report is None:
        return {"error": f"Niepoprawny SMILES: {smiles!r}"}

    render_molecule_image(report["smiles"])

    return {
        "smiles": report["smiles"],
        "molecular_weight_g_per_mol": round(report["mw_freebase"], 2),
        "logP": round(report["alogp"], 2),
        "h_bond_acceptors": int(report["hba"]),
        "h_bond_donors": int(report["hbd"]),
        "tpsa_A2": round(report["psa"], 2),
        "rotatable_bonds": int(report["rtb"]),
        "lipinski_ro5_violations": int(report["num_ro5_violations"]),
        "num_atoms": report["num_atoms"],
        "num_bonds": report["num_bonds"],
        "num_rings": report["num_rings"],
    }


TOOL_IMPLEMENTATIONS = {
    "evaluate_pic50": evaluate_pic50,
    "get_chem_properties": get_chem_properties,
}

TOOLS_SPEC = [
    {
        "type": "function",
        "function": {
            "name": "evaluate_pic50",
            "description": (
                "Przewiduje pIC50 (logarytm aktywnosci biologicznej) podanej czasteczki "
                "uzywajac wewnetrznego modelu HybridGINE wytrenowanego na ChEMBL."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "smiles": {"type": "string", "description": "Kanoniczny SMILES czasteczki"},
                    "standard_type": {
                        "type": "string",
                        "enum": ALLOWED_STANDARD_TYPES,
                        "description": "Typ pomiaru aktywnosci (domyslnie IC50)",
                    },
                    "bao_format": {
                        "type": "string",
                        "enum": ALLOWED_BAO_FORMATS,
                        "description": "Format BAO assayu (domyslnie BAO_0000219 - single protein format)",
                    },
                    "organism": {
                        "type": "string",
                        "enum": ALLOWED_ORGANISMS,
                        "description": "Organizm targetu (domyslnie Homo sapiens)",
                    },
                },
                "required": ["smiles"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_chem_properties",
            "description": (
                "Zwraca podstawowe wlasciwosci chemiczne (masa, LogP, HBA/HBD, TPSA, "
                "rotatable bonds, naruszenia regul Lipinskiego) wyliczone przez RDKit."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "smiles": {"type": "string", "description": "Kanoniczny SMILES czasteczki"},
                },
                "required": ["smiles"],
            },
        },
    },
]

SYSTEM_PROMPT = (
    "Jesteś ekspertem chemii medycznej i asystentem AI ds. odkrywania leków. Twoim zadaniem jest analiza cząsteczek podanych w formacie SMILES.\n\n"
    "TWOJE NARZĘDZIA:\n"
    "- `evaluate_pic50`: Przewiduje aktywność biologiczną (pIC50).\n"
    "- `get_chem_properties`: Wylicza właściwości fizykochemiczne (ADME).\n\n"
    "PROCEDURA DZIAŁANIA (ZAWSZE PRZESTRZEGAJ):\n"
    "1. Gdy otrzymasz SMILES, ZAWSZE wywołaj OBA narzędzia równolegle.\n"
    "2. Jeśli użytkownik poda nazwę i SMILES, ZAWSZE weryfikuj czy SMILES odpowiada tej nazwie. Jeśli SMILES nie odpowiada nazwie, napisz: 'Podany SMILES nie odpowiada <nazwa>, analizuję faktyczną strukturę chemiczną'.\n"
    "3. CZEKAJ na wyniki z narzędzi. NIE generuj odpowiedzi przed otrzymaniem danych.\n"
    "4. SYNTEZA WYNIKÓW:\n"
    "   - Aktywność: pIC50 > 6.0 (dobra), pIC50 > 7.0 (bardzo wysoka).\n"
    "   - Reguła Lipińskiego: Jeśli `num_ro5_violations` > 0, wyraźnie wskaż naruszenia.\n"
    "   - Styl: Używaj języka polskiego, bądź zwięzły, merytoryczny i konkretny.\n"
    "5. ZASADA BEZPIECZEŃSTWA: Jeśli narzędzie zwróci błąd, poinformuj o tym użytkownika zamiast zgadywać wyniki.\n"
    "\n\nKRYTYCZNE ZASADY CHEMICZNE:"
    "\n- Jeśli użytkownik poda tylko nazwę związku (np. 'aspiryna'), a NIE poda SMILES-a, "
    "NIE ZGADUJ STRUKTURY. Zamiast tego odpisz: 'Nie znam dokładnego zapisu SMILES dla tego związku. "
    "Proszę, podaj kod SMILES, abym mógł wykonać predykcję'."
    "\n- Nigdy nie przypisuj losowego SMILES-a do nazwy związku, której nie jesteś pewien."
    "\n- Jeśli pIC50 wynosi X, napisz: 'Wartość pIC50 wynosi X'. Nie dodawaj od siebie czy to 'dobry' czy 'zły' potencjał, chyba że jest to bezpośrednia interpretacja wyniku zgodnie z moimi wytycznymi."
)


# ---------------------------------------------------------------------------

st.title("🧪 AI Drug Discovery Agent")
st.caption(f"HybridGINE (pure topology) | urzadzenie: `{device}` | wagi: `{MODEL_PATH.name}`")

with st.sidebar:
    st.header("Konfiguracja")
    base_url_input = st.text_input(
        "Ollama base URL",
        value=os.environ.get("OLLAMA_BASE_URL", LLM_BASE_URL),
        help="Endpoint OpenAI-compatible serwera Ollama (domyslnie http://localhost:11434/v1).",
    )
    model_input = st.text_input(
        "Model Ollama",
        value=os.environ.get("OLLAMA_MODEL", LLM_MODEL),
        help="Musi byc dostepny w `ollama list`. Dla tool-calling: llama3.1, qwen2.5, mistral-nemo.",
    )
    api_key_input = st.text_input(
        "Gemini/OpenAI API key",
        value=os.environ.get("OPENAI_API_KEY", LLM_API_KEY_DEFAULT),
        type="password",
        help="Dla lokalnej Ollamy klucz nie jest weryfikowany - wystarczy dowolny string (np. 'ollama').",
    )
    st.caption(f"LLM: `{model_input}` @ `{base_url_input}`")

    st.divider()
    st.subheader("Kontekst predykcji pIC50")
    st.session_state.setdefault("standard_type", DEFAULT_STANDARD_TYPE)
    st.session_state.setdefault("bao_format", DEFAULT_BAO_FORMAT)
    st.session_state.setdefault("organism", DEFAULT_ORGANISM)

    st.selectbox("standard_type", ALLOWED_STANDARD_TYPES,
                 index=ALLOWED_STANDARD_TYPES.index(st.session_state["standard_type"]),
                 key="standard_type")
    st.selectbox("bao_format", ALLOWED_BAO_FORMATS,
                 index=ALLOWED_BAO_FORMATS.index(st.session_state["bao_format"]),
                 key="bao_format")
    st.selectbox("organism", ALLOWED_ORGANISMS,
                 index=ALLOWED_ORGANISMS.index(st.session_state["organism"]),
                 key="organism")

    st.divider()
    if st.button("🧹 Wyczysc czat"):
        st.session_state.messages = []
        st.rerun()

    with st.expander("Przyklady SMILES"):
        st.code("CC(=O)Oc1ccccc1C(=O)O   # aspiryna", language="text")
        st.code("CN1C=NC2=C1C(=O)N(C(=O)N2C)C   # kofeina", language="text")
        st.code("CC(C)Cc1ccc(cc1)C(C)C(=O)O   # ibuprofen", language="text")
        st.code("Cc1ccc2nc(-c3ccc(NS(C)(=O)=O)cc3)sc2c1   # CHEMBL imatinib-like", language="text")

if "messages" not in st.session_state:
    st.session_state.messages = []


def _strip_ctx(text: str) -> str:
    """Ukryj dolaczony kontekst sidebar przy wyswietlaniu user message."""
    marker = "\n\n(Domyslny kontekst predykcji:"
    return text.split(marker, 1)[0] if text else text


def render_history() -> None:
    """Renderuje historie chatu (pomija wewnetrzne assistant tool_call placeholders)."""
    for msg in st.session_state.messages:
        role = msg.get("role")
        if role == "user":
            with st.chat_message("user"):
                st.markdown(_strip_ctx(msg["content"]))
        elif role == "assistant" and msg.get("content"):
            with st.chat_message("assistant"):
                st.markdown(msg["content"])
        elif role == "tool":
            with st.chat_message("assistant"):
                payload = None
                try:
                    payload = json.loads(msg["content"])
                except Exception:  # noqa: BLE001
                    pass

                if (
                    msg.get("name") == "get_chem_properties"
                    and isinstance(payload, dict)
                    and "smiles" in payload
                ):
                    render_molecule_image(payload["smiles"])

                with st.expander(f"🔧 narzedzie: {msg.get('name', '?')}", expanded=False):
                    if payload is not None:
                        st.json(payload)
                    else:
                        st.code(msg["content"])


render_history()


def _history_to_llm_messages() -> list[dict]:
    """Konwertuje st.session_state.messages na format messages dla LLM (OpenAI SDK)."""
    out: list[dict] = [{"role": "system", "content": SYSTEM_PROMPT}]
    for m in st.session_state.messages:
        role = m.get("role")
        if role == "user":
            out.append({"role": "user", "content": m["content"]})
        elif role == "assistant":
            entry: dict = {"role": "assistant", "content": m.get("content")}
            if m.get("tool_calls"):
                entry["tool_calls"] = m["tool_calls"]
            out.append(entry)
        elif role == "tool":
            out.append({
                "role": "tool",
                "tool_call_id": m["tool_call_id"],
                "content": m["content"],
            })
    return out


def run_agent(client: OpenAI, model_name: str, user_prompt: str) -> str:
    """Petla narzedziowa LLM: model -> tool calls -> model -> ... -> final."""
    ctx = (
        f"\n\n(Domyslny kontekst predykcji: standard_type={st.session_state['standard_type']}, "
        f"bao_format={st.session_state['bao_format']}, organism={st.session_state['organism']})"
    )
    st.session_state.messages.append({"role": "user", "content": user_prompt + ctx})

    convo = _history_to_llm_messages()

    for round_idx in range(6):  # max 6 rund narzedziowych
        print(f"DEBUG: [TERMINAL] Runda {round_idx + 1} -> wywoluje LLM `{model_name}`", flush=True)
        response = client.chat.completions.create(
            model=model_name,
            messages=convo,
            tools=TOOLS_SPEC,
            tool_choice="auto",
            temperature=0.2,
        )
        msg = response.choices[0].message

        if not msg.tool_calls:
            final = msg.content or ""
            print(f"DEBUG: [TERMINAL] Brak tool_calls - koncze. Dlugosc odpowiedzi: {len(final)}",
                  flush=True)
            st.session_state.messages.append({"role": "assistant", "content": final})
            return final

        print(f"DEBUG: [TERMINAL] LLM zazadal {len(msg.tool_calls)} wywolan narzedzi",
              flush=True)

        tool_calls_payload = [
            {
                "id": tc.id,
                "type": "function",
                "function": {"name": tc.function.name, "arguments": tc.function.arguments},
            }
            for tc in msg.tool_calls
        ]
        assistant_entry = {
            "role": "assistant",
            "content": msg.content,
            "tool_calls": tool_calls_payload,
        }
        convo.append(assistant_entry)
        st.session_state.messages.append(assistant_entry)

        for tc in msg.tool_calls:
            name = tc.function.name
            print(f"DEBUG: [TERMINAL] Wywoluje narzedzie: {name}", flush=True)
            try:
                args = json.loads(tc.function.arguments or "{}")
            except json.JSONDecodeError:
                print(f"DEBUG: [TERMINAL] Nie udalo sie sparsowac argumentow: "
                      f"{tc.function.arguments!r}", flush=True)
                args = {}

            print(f"DEBUG: [TERMINAL]   args = {args}", flush=True)

            fn = TOOL_IMPLEMENTATIONS.get(name)
            if fn is None:
                print(f"DEBUG: [TERMINAL]   Nieznane narzedzie: {name}", flush=True)
                result = {"error": f"Nieznane narzedzie: {name}"}
            else:
                # Wstrzykuj domyslny kontekst gdy LLM go pominal
                if name == "evaluate_pic50":
                    args.setdefault("standard_type", st.session_state["standard_type"])
                    args.setdefault("bao_format", st.session_state["bao_format"])
                    args.setdefault("organism", st.session_state["organism"])
                try:
                    result = fn(**args)
                except TypeError as exc:
                    print(f"DEBUG: [TERMINAL]   Zle argumenty {name}: {exc}", flush=True)
                    result = {"error": f"Zle argumenty: {exc}"}
                except Exception as exc:
                    print(f"DEBUG: [TERMINAL]   Wyjatek w {name}: {exc!r}", flush=True)
                    result = {"error": f"Wyjatek narzedzia: {exc}"}

            result_text = json.dumps(result, ensure_ascii=False)
            print(f"DEBUG: [TERMINAL]   {name} -> {result_text[:200]}"
                  + (" ..." if len(result_text) > 200 else ""), flush=True)
            tool_msg = {
                "role": "tool",
                "tool_call_id": tc.id,
                "name": name,
                "content": result_text,
            }
            convo.append({k: v for k, v in tool_msg.items() if k != "name"})
            st.session_state.messages.append(tool_msg)

    fallback = "Przekroczono limit iteracji narzedzi."
    print(f"DEBUG: [TERMINAL] {fallback}", flush=True)
    st.session_state.messages.append({"role": "assistant", "content": fallback})
    return fallback


if prompt := st.chat_input("Podaj SMILES czasteczki albo zadaj pytanie..."):
    if not base_url_input or not model_input:
        st.error("Wypelnij `Ollama base URL` i `Model Ollama` w panelu bocznym.")
        st.stop()

    client = OpenAI(api_key=api_key_input or LLM_API_KEY_DEFAULT, base_url=base_url_input)

    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner(f"Agent mysli ({model_input})..."):
            try:
                answer = run_agent(client, model_input, prompt)
            except Exception as exc:  # noqa: BLE001
                answer = (
                    f"Blad agenta: {exc}\n\n"
                    f"Sprawdz czy Ollama dziala (`ollama serve`) i czy model `{model_input}` "
                    f"jest pobrany (`ollama pull {model_input}`)."
                )
                st.session_state.messages.append({"role": "assistant", "content": answer})
        st.markdown(answer)
