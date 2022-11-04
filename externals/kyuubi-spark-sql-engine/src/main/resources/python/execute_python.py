import ast
import sys
import io
import json
import traceback
import re


if sys.version >= '3':
    unicode = str
else:
    import cStringIO
    import StringIO

TOP_FRAME_REGEX = re.compile(r'\s*File "<stdin>".*in <module>')

global_dict = {}

class NormalNode(object):
    def __init__(self, code):
        self.code = compile(code, '<stdin>', 'exec', ast.PyCF_ONLY_AST, 1)

    def execute(self):
        to_run_exec, to_run_single = self.code.body[:-1], self.code.body[-1:]

        try:
            for node in to_run_exec:
                mod = ast.Module([node])
                code = compile(mod, '<stdin>', 'exec')
                exec(code, global_dict)

            for node in to_run_single:
                mod = ast.Interactive([node])
                code = compile(mod, '<stdin>', 'single')
                exec(code, global_dict)
        except:
            # We don't need to log the exception because we're just executing user
            # code and passing the error along.
            raise ExecutionError(sys.exc_info())

class ExecutionError(Exception):
    def __init__(self, exc_info):
        self.exc_info = exc_info

class UnicodeDecodingStringIO(io.StringIO):
    def write(self, s):
        if isinstance(s, bytes):
            s = s.decode("utf-8")
        super(UnicodeDecodingStringIO, self).write(s)

def clearOutputs():
    sys.stdout.close()
    sys.stderr.close()
    sys.stdout = UnicodeDecodingStringIO()
    sys.stderr = UnicodeDecodingStringIO()


def parse_code_into_nodes(code):
    nodes = []
    try:
        nodes.append(NormalNode(code))
    except SyntaxError:
        # It's possible we hit a syntax error because of a magic command. Split the code groups
        # of 'normal code', and code that starts with a '%'. possibly magic code
        # lines, and see if any of the lines
        # Remove lines until we find a node that parses, then check if the next line is a magic
        # line
        # .

        # Split the code into chunks of normal code, and possibly magic code, which starts with
        # a '%'.

        normal = []
        chunks = []
        for i, line in enumerate(code.rstrip().split('\n')):
            if line.startswith('%'):
                if normal:
                    chunks.append('\n'.join(normal))
                    normal = []

                chunks.append(line)
            else:
                normal.append(line)

        if normal:
            chunks.append('\n'.join(normal))

        # Convert the chunks into AST nodes. Let exceptions propagate.
        for chunk in chunks:
            if chunk.startswith('%'):
                nodes.append(MagicNode(chunk))
            else:
                nodes.append(NormalNode(chunk))

    return nodes

def execute_reply(status, content):
    msg = {
        'msg_type': 'execute_reply',
        'content': dict(
            content,
            status=status,
        )
    }
    return json.dumps(msg)

def execute_reply_ok(data):
    return execute_reply("ok", {
        "data": data,
    })

def execute_reply_error(exc_type, exc_value, tb):
    # LOG.error('execute_reply', exc_info=True)
    if sys.version >= '3':
      formatted_tb = traceback.format_exception(exc_type, exc_value, tb, chain=False)
    else:
      formatted_tb = traceback.format_exception(exc_type, exc_value, tb)
    for i in range(len(formatted_tb)):
        if TOP_FRAME_REGEX.match(formatted_tb[i]):
            formatted_tb = formatted_tb[:1] + formatted_tb[i + 1:]
            break

    return execute_reply('error', {
        'ename': unicode(exc_type.__name__),
        'evalue': unicode(exc_value),
        'traceback': formatted_tb,
    })

def execute_request(content):
    try:
        code = content['code']
    except KeyError:
        return execute_reply_internal_error(
            'Malformed message: content object missing "code"', sys.exc_info()
        )

    try:
        nodes = parse_code_into_nodes(code)
    except SyntaxError:
        exc_type, exc_value, tb = sys.exc_info()
        return execute_reply_error(exc_type, exc_value, None)

    result = None

    try:
        for node in nodes:
            result = node.execute()
    except ExecutionError as e:
        return execute_reply_error(*e.exc_info)

    if result is None:
        result = {}

    stdout = sys.stdout.getvalue()
    stderr = sys.stderr.getvalue()

    clearOutputs()

    output = result.pop('text/plain', '')

    if stdout:
        output += stdout

    if stderr:
        output += stderr

    output = output.rstrip()

    # Only add the output if it exists, or if there are no other mimetypes in the result.
    if output or not result:
        result['text/plain'] = output.rstrip()

    return execute_reply_ok(result)

import findspark
findspark.init()
import kyuubi_util
spark = kyuubi_util.get_spark()
global_dict['spark'] = spark

def main():
    sys_stdin = sys.stdin
    sys_stdout = sys.stdout
    sys_stderr = sys.stderr

    if sys.version >= '3':
        sys.stdin = io.StringIO()
    else:
        sys.stdin = cStringIO.StringIO()

    sys.stdout = UnicodeDecodingStringIO()
    sys.stderr = UnicodeDecodingStringIO()

    stderr = sys.stderr.getvalue()
    print(stderr, file=sys_stderr)
    clearOutputs
    try:

        while True:
            line = sys_stdin.readline()

            if line == '':
                break
            elif line == '\n':
                continue

            try:
                content = json.loads(line)
            except ValueError:
                # LOG.error('failed to parse message', exc_info=True)
                continue

            if content['cmd'] == 'exit_worker':
                break

            result = execute_request(content)
            print(result, file=sys_stdout)
            sys_stdout.flush()
            clearOutputs()
    finally:
        print("python worker exit", file=sys_stderr)
        sys.stdin = sys_stdin
        sys.stdout = sys_stdout
        sys.stderr = sys_stderr

if __name__ == '__main__':
    sys.exit(main())