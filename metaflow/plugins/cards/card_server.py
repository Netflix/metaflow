import os
import json
from http.server import BaseHTTPRequestHandler

try:
    from http.server import ThreadingHTTPServer
except ImportError:
    from socketserver import ThreadingMixIn
    from http.server import HTTPServer

    class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
        daemon_threads = True


from .card_client import CardContainer
from .exception import CardNotPresentException
from .card_resolver import resolve_paths_from_task

VIEWER_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "card_viewer", "viewer.html"
)

CARD_VIEWER_HTML = open(VIEWER_PATH).read()

TASK_CACHE = {}


class CardServerOptions:
    def __init__(
        self, run_object, only_running, follow_resumed, flow_datastore, max_cards=20
    ):
        self.run_object = run_object
        self.only_running = only_running
        self.follow_resumed = follow_resumed
        self.flow_datastore = flow_datastore
        self.max_cards = max_cards


def cards_for_task(
    flow_datastore, task_pathspec, card_type=None, card_hash=None, card_id=None
):
    try:
        paths, card_ds = resolve_paths_from_task(
            flow_datastore,
            task_pathspec,
            type=card_type,
            hash=card_hash,
            card_id=card_id,
        )
    except CardNotPresentException:
        return None
    for card in CardContainer(paths, card_ds, from_resumed=False, origin_pathspec=None):
        yield card


def cards_for_run(
    flow_datastore,
    run_object,
    only_running,
    card_type=None,
    card_hash=None,
    card_id=None,
    max_cards=20,
):
    curr_idx = 0
    for step in run_object.steps():
        for task in step.tasks():
            if only_running and task.finished:
                continue
            card_generator = cards_for_task(
                flow_datastore,
                task.pathspec,
                card_type=card_type,
                card_hash=card_hash,
                card_id=card_id,
            )
            if card_generator is None:
                continue
            for card in card_generator:
                curr_idx += 1
                if curr_idx >= max_cards:
                    raise StopIteration
                yield task.pathspec, card


class CardViewerRoutes(BaseHTTPRequestHandler):

    card_options: CardServerOptions = None

    def do_GET(self):
        try:
            _, path = self.path.split("/", 1)
            try:
                prefix, suffix = path.split("/", 1)
            except:
                prefix = path
                suffix = None
        except:
            prefix = None
        if prefix in self.ROUTES:
            self.ROUTES[prefix](self, suffix)
        else:
            self._response(CARD_VIEWER_HTML.encode("utf-8"))

    def get_runinfo(self, suffix):
        task_card_generator = cards_for_run(
            self.card_options.flow_datastore,
            self.card_options.run_object,
            self.card_options.only_running,
            max_cards=self.card_options.max_cards,
        )
        flow_name = self.card_options.run_object.parent.id
        run_id = self.card_options.run_object.id
        cards = []
        for pathspec, card in task_card_generator:
            step, task = pathspec.split("/")[-2:]
            cards.append(
                dict(
                    task=pathspec,
                    label="%s/%s %s" % (step, task, card.hash),
                    card_object=dict(
                        hash=card.hash,
                        type=card.type,
                        path=card.path,
                        id=card.id,
                    ),
                    card="%s/%s" % (pathspec, card.hash),
                )
            )
        resp = {"status": "ok", "flow": flow_name, "run_id": run_id, "cards": cards}
        self._response(resp, is_json=True)

    def get_card(self, suffix):
        flow, run_id, step, task_id, card_hash = suffix.split("/")
        pathspec = "/".join([flow, run_id, step, task_id])
        cards = list(
            cards_for_task(
                self.card_options.flow_datastore, pathspec, card_hash=card_hash
            )
        )
        if len(cards) == 0:
            self._response("Card not found", code=404)
            return
        selected_card = cards[0]
        self._response(selected_card.get().encode("utf-8"))

    def get_data(self, suffix):
        flow, run_id, step, task_id, card_hash = suffix.split("/")
        pathspec = "/".join([flow, run_id, step, task_id])
        cards = list(
            cards_for_task(
                self.card_options.flow_datastore, pathspec, card_hash=card_hash
            )
        )
        if len(cards) == 0:
            self._response("Card not found", code=404)
            return
        selected_card = cards[0]
        card_data = selected_card._get_data()
        if card_data is not None:
            self._response({"status": "ok", "payload": card_data}, is_json=True)
        else:
            self._response({"status": "not found"}, is_json=True)

    def _response(self, body, is_json=False, code=200):
        self.send_response(code)
        mime = "application/json" if is_json else "text/html"
        self.send_header("Content-type", mime)
        self.end_headers()
        if is_json:
            self.wfile.write(json.dumps(body).encode("utf-8"))
        else:
            self.wfile.write(body)

    ROUTES = {"runinfo": get_runinfo, "card": get_card, "data": get_data}


def create_card_server(card_options, port, ctx_obj):
    CardViewerRoutes.card_options = card_options
    server_addr = ("", port)
    ctx_obj.echo(
        "Starting card server on port %d for run-id %s"
        % (port, str(card_options.run_object.pathspec)),
        fg="green",
        bold=True,
    )
    server = ThreadingHTTPServer(server_addr, CardViewerRoutes)
    server.serve_forever()
