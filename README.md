"# test_workflow

## Workflow implementati

### 1. Convert Python to Java (GitHub Models API)

**File:** `.github/workflows/python-to-java.yml`

Converte automaticamente i file Python modificati in equivalenti file Java usando l'API di GitHub Models (modello Claude). Il workflow:

1. Si attiva ad ogni push che modifica file `.py`
2. Rileva i file Python aggiunti o modificati nell'ultimo commit
3. Per ogni file, chiama l'API di GitHub Models per generare il codice Java equivalente
4. Committa e pusha i file `.java` direttamente sul branch

**Configurazione:**

1. Creare un Personal Access Token (PAT) con accesso a GitHub Models:
   - Andare su **GitHub → Settings → Developer settings → Personal access tokens → Fine-grained tokens**
   - Creare un nuovo token con il permesso **Models (read)**
2. Aggiungere il token come secret del repository:
   - Andare su **Settings → Secrets and variables → Actions → New repository secret**
   - Nome: `GH_MODELS_TOKEN`
   - Valore: il PAT creato al passo precedente

> **Nota:** questo workflow committa direttamente sul branch senza review.

---

### 2. Convert Python to Java (Copilot Coding Agent)

**File:** `.github/workflows/python-to-java-agent.yml`

Converte i file Python in Java delegando il lavoro al Copilot coding agent. Il workflow:

1. Si attiva ad ogni push che modifica file `.py`
2. Rileva i file Python aggiunti o modificati nell'ultimo commit
3. Crea una GitHub Issue con le istruzioni di conversione e la assegna a `copilot`
4. Il coding agent legge i file, esegue la conversione e apre una Pull Request

**Configurazione:**

1. Abilitare il Copilot coding agent nel repository:
   - Andare su **Settings → Copilot → Coding agent**
   - Attivare l'opzione
2. Nessun secret aggiuntivo richiesto — il workflow usa il `GITHUB_TOKEN` built-in fornito automaticamente da GitHub Actions

> **Nota:** questo workflow produce una PR che può essere rivista prima del merge.

---

### Quale scegliere?

| | GitHub Models | Coding Agent |
|---|---|---|
| **Output** | Commit diretto | Pull Request |
| **Review** | Nessuna | Possibile prima del merge |
| **Secret richiesto** | `GH_MODELS_TOKEN` (PAT) | Nessuno |
| **Contesto** | Solo il singolo file | Accesso completo al repo |
| **Velocità** | Più veloce (singola API call) | Più lento (agent asincrono) |

> **Attenzione:** entrambi i workflow si attivano sullo stesso evento (`push` di file `.py`). Per evitare esecuzioni duplicate, disabilitare quello che non si intende usare dal tab **Actions** del repository, oppure spostare uno dei due su un branch diverso.

---

## TODO

- [x] Coding agent — workflow per convertire Python in Java tramite Copilot coding agent
- [ ] Test globali — porting dei test da Java verso Python
- [ ] Workflow per ottimizzazione del codice
- [ ] Workflow per bug fix e TODO per sviluppatore" 
