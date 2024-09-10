import React, { FC, useEffect, useState } from "react";
import { createSnippet } from "./snippets.fixture";
import AceEditor from "react-ace";
import "ace-builds/webpack-resolver";
import "ace-builds/src-noconflict/ext-language_tools";
import "ace-builds/src-noconflict/mode-javascript";
import "ace-builds/src-noconflict/mode-python";
import "ace-builds/src-noconflict/mode-plain_text";
import "ace-builds/src-noconflict/theme-monokai";
import Select from "components/Elements/Selectv2/Select";

export enum SnippetMode {
  JS_FETCH,
  NODEJS_AXIOS,
  PYTHON_HTTP_CLIENT,
  CURL,
}

export type EditorType = "javascript" | "python" | "plain_text";

const snippetModeToEditorModeMap: Record<SnippetMode, EditorType> = {
  [SnippetMode.JS_FETCH]: "javascript",
  [SnippetMode.NODEJS_AXIOS]: "javascript",
  [SnippetMode.PYTHON_HTTP_CLIENT]: "python",
  [SnippetMode.CURL]: "plain_text",
};

export interface SnippetPickerProps {
  userApiKey: string;
  email: string;
  firstName: string;
  lastName: string;
}

const SnippetPicker: FC<SnippetPickerProps> = ({
  userApiKey,
  email,
  firstName,
  lastName,
}) => {
  const [snippet, setSnippet] = useState("");
  const [snippetMode, setSnippetMode] = useState(SnippetMode.JS_FETCH);

  useEffect(() => {
    setSnippet(
      createSnippet(userApiKey, firstName, lastName, email, snippetMode)
    );
  }, [userApiKey, snippetMode]);

  return (
    <div className="p-5 rounded-lg bg-[#F3F4F6] flex flex-col gap-5">
      <Select
        className="max-w-[200px]"
        options={[
          {
            key: SnippetMode.JS_FETCH,
            title: "Javascript - Fetch",
          },
          {
            key: SnippetMode.NODEJS_AXIOS,
            title: "Node.js - Axios",
          },
          {
            key: SnippetMode.PYTHON_HTTP_CLIENT,
            title: "Python - http.client",
          },
          { key: SnippetMode.CURL, title: "cURL" },
        ]}
        onChange={(val) => setSnippetMode(val)}
        value={snippetMode}
      />
      <AceEditor
        className="rounded-lg"
        aria-label="editor"
        mode={snippetModeToEditorModeMap[snippetMode]}
        theme="monokai"
        name="editor"
        fontSize={12}
        minLines={15}
        maxLines={40}
        width="100%"
        showPrintMargin={false}
        showGutter
        placeholder="Write your Query here..."
        editorProps={{ $blockScrolling: true }}
        setOptions={{
          enableBasicAutocompletion: true,
          enableLiveAutocompletion: true,
          enableSnippets: true,
        }}
        value={snippet}
        onChange={(val) => setSnippet(val)}
      />
    </div>
  );
};

export default SnippetPicker;
