import sys
import os
import codecs
import chardet
import tempfile
import shutil
import errno

class Config:
    MAX_FILE_SIZE = 100 * 1024 * 1024
    CHUNK_SIZE = 65536
    DETECTION_SAMPLE_SIZE = 65536
    COMPATIBILITY_CHECK_SIZE = 1024
    SJIS_SUFFIX = '_sjis'
    SJISX_SUFFIX = '_sjisx'
    ENCODING_SHIFT_JIS = 'SHIFT_JIS'
    ENCODING_UTF8 = 'UTF-8'
    ENCODING_UTF8_SIG_BOM = 'UTF-8-SIG（BOMあり）'
    DEFAULT_READ_ENCODING_FOR_ASCII = 'utf-8'

def is_binary_file(filepath):
    try:
        with open(filepath, 'rb') as f:
            chunk = f.read(1024)
            if not chunk:
                return False

            if chunk.startswith(b'\xff\xfe') or chunk.startswith(b'\xfe\xff'):
                return False

            return b'\x00' in chunk
    except Exception:
        return True

def has_bom_utf8(filepath):
    try:
        with open(filepath, 'rb') as f:
            bom = f.read(3)
            return bom == b'\xef\xbb\xbf'
    except Exception:
        return False

def normalize_encoding_name(encoding):
    if not encoding:
        return encoding

    encoding_lower = encoding.lower()

    mapping = {
        'utf-8': Config.ENCODING_UTF8, 'utf8': Config.ENCODING_UTF8, 'utf-8-sig': Config.ENCODING_UTF8_SIG_BOM, 'utf8-sig': Config.ENCODING_UTF8_SIG_BOM,
        'utf-16le': 'UTF-16LE', 'utf16le': 'UTF-16LE',
        'utf-16be': 'UTF-16BE', 'utf16be': 'UTF-16BE',
        'euc-jp': 'EUC-JP', 'eucjp': 'EUC-JP',
        'iso-2022-jp': 'ISO-2022-JP', 'iso2022jp': 'ISO-2022-JP',
        'shift_jis': Config.ENCODING_SHIFT_JIS, 'shift-jis': Config.ENCODING_SHIFT_JIS, 'sjis': Config.ENCODING_SHIFT_JIS, 'cp932': Config.ENCODING_SHIFT_JIS, 'windows-31j': Config.ENCODING_SHIFT_JIS,
        'windows-1252': 'WINDOWS-1252',
        'iso-8859-1': 'ISO-8859-1',
    }

    if encoding_lower in mapping:
        return mapping[encoding_lower]

    return encoding.upper()

def test_utf8_decode(raw_data):
    try:
        raw_data.decode('utf-8')
        return True
    except UnicodeDecodeError:
        return False

def detect_encoding(filepath):
    try:
        file_size = os.path.getsize(filepath)
        if file_size > Config.MAX_FILE_SIZE:
            return None, f"ファイルサイズが大きすぎます（{file_size // (1024*1024)}MB）"

        if file_size == 0:
            return None, "空ファイルです"

        sample_size = min(file_size, Config.DETECTION_SAMPLE_SIZE)
        with open(filepath, 'rb') as f:
            raw_data = f.read(sample_size)

        if not raw_data:
            return None, "空ファイルです"

        if raw_data.startswith(b'\xff\xfe'):
            return 'UTF-16LE', None
        elif raw_data.startswith(b'\xfe\xff'):
            return 'UTF-16BE', None

        has_utf8_bom = raw_data.startswith(b'\xef\xbb\xbf')
        if has_utf8_bom:
            if test_utf8_decode(raw_data[3:]):
                return Config.ENCODING_UTF8_SIG_BOM, None
            else:
                return None, "UTF-8 BOMがありますが、内容が不正です"

        if is_binary_file(filepath):
            return None, "バイナリファイルです"

        data_for_chardet = raw_data[3:] if has_utf8_bom else raw_data
        result = chardet.detect(data_for_chardet)

        if result is None or result.get('encoding') is None:
            return None, "文字コードを検出できませんでした"

        detected_chardet_encoding = result['encoding']
        confidence = result.get('confidence', 0)

        normalized_encoding = normalize_encoding_name(detected_chardet_encoding)

        if normalized_encoding in [Config.ENCODING_UTF8, 'ASCII']:
            if test_utf8_decode(raw_data):
                return Config.ENCODING_UTF8, None

        if confidence > 0.8:
            if normalized_encoding == Config.ENCODING_SHIFT_JIS and test_utf8_decode(raw_data):
                return Config.ENCODING_UTF8, None
            return normalized_encoding, None

        return None, f"文字コードの検出信頼性が低いです ({detected_chardet_encoding}, confidence={confidence:.2f})"

    except PermissionError:
        return None, "ファイルにアクセスできません"
    except Exception as e:
        return None, f"エンコーディング検出エラー: {str(e)}"

def get_read_encoding(encoding):
    if encoding == Config.ENCODING_UTF8_SIG_BOM:
        return 'utf-8-sig'
    elif encoding == Config.ENCODING_UTF8:
        return 'utf-8'
    elif encoding == Config.ENCODING_SHIFT_JIS:
        return 'cp932'
    elif encoding == 'EUC-JP':
        return 'euc-jp'
    elif encoding == 'ISO-2022-JP':
        return 'iso-2022-jp'
    elif encoding == 'WINDOWS-1252':
        return 'windows-1252'
    elif encoding == 'ISO-8859-1':
        return 'iso-8859-1'
    elif encoding == 'ASCII':
        return Config.DEFAULT_READ_ENCODING_FOR_ASCII
    else:
        return encoding.lower().replace('-', '_')

def check_char_sjis_compatibility(char):
    try:
        encoded = char.encode('shift_jis')
        decoded = encoded.decode('shift_jis')
        return char == decoded
    except (UnicodeEncodeError, UnicodeDecodeError):
        return False

def check_sjis_compatibility_stream(filepath, encoding):
    try:
        read_encoding = get_read_encoding(encoding)
        incompatible_found = False

        with codecs.open(filepath, 'r', encoding=read_encoding, errors='replace') as infile:
            while True:
                chunk = infile.read(Config.COMPATIBILITY_CHECK_SIZE)
                if not chunk:
                    break

                for char in chunk:
                    if not check_char_sjis_compatibility(char):
                        incompatible_found = True
                        break

                if incompatible_found:
                    break

        return incompatible_found

    except Exception:
        return True

def generate_output_filename(filepath, has_incompatible_chars):
    try:
        abs_filepath = os.path.abspath(filepath)
        original_dir = os.path.dirname(abs_filepath)
        
        if not os.path.exists(original_dir):
            original_dir = os.getcwd()
            
        filename = os.path.basename(abs_filepath)
        name, ext = os.path.splitext(filename)
        
        if has_incompatible_chars:
            new_filename = name + Config.SJISX_SUFFIX + ext
        else:
            new_filename = name + Config.SJIS_SUFFIX + ext
            
        return os.path.join(original_dir, new_filename), new_filename
        
    except Exception as e:
        print(f"Error in generate_output_filename for {filepath}: {str(e)}", file=sys.stderr)
        raise Exception(f"出力ファイル名の生成に失敗しました: {str(e)}")

def create_temp_file_safely(output_dir):
    temp_fd = -1
    temp_filepath = None
    
    try:
        if not os.path.isdir(output_dir):
            try:
                os.makedirs(output_dir, exist_ok=True)
            except OSError as e:
                 raise Exception(f"一時ファイル用ディレクトリの作成に失敗: {output_dir}, {str(e)}")

        temp_fd, temp_filepath = tempfile.mkstemp(
            suffix='.tmp', 
            prefix='sjis_conv_', 
            dir=output_dir
        )
        os.close(temp_fd)
        temp_fd = -1
        return temp_filepath
        
    except Exception as e:
        if temp_fd != -1:
            try:
                os.close(temp_fd)
            except OSError:
                pass
        if temp_filepath and os.path.exists(temp_filepath):
            try:
                os.remove(temp_filepath)
            except OSError:
                pass
        raise Exception(f"一時ファイルの作成に失敗: {str(e)}")

def convert_file_stream(input_filepath, output_filepath, encoding):
    read_encoding = get_read_encoding(encoding)
    temp_file = None

    try:
        output_dir = os.path.dirname(output_filepath)
        if not output_dir:
            output_dir = os.getcwd()

        temp_file = create_temp_file_safely(output_dir)

        write_encoding = 'shift_jis'

        with codecs.open(input_filepath, 'r', encoding=read_encoding, errors='replace') as infile, \
             codecs.open(temp_file, 'w', encoding=write_encoding, errors='replace') as outfile:

            while True:
                chunk = infile.read(Config.CHUNK_SIZE)
                if not chunk:
                    break
                outfile.write(chunk)

        backup_file = None
        if os.path.exists(output_filepath):
            backup_file = output_filepath + '.backup'
            if os.path.exists(backup_file):
                try:
                    os.remove(backup_file)
                except OSError as e:
                    pass

            try:
                shutil.move(output_filepath, backup_file)
            except FileNotFoundError:
                backup_file = None
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise
                backup_file = None

        try:
            shutil.move(temp_file, output_filepath)
        except OSError as e:
            if hasattr(e, 'errno') and e.errno == errno.EEXIST:
                raise Exception(f"Output file {output_filepath} already exists.")
            raise Exception(f"Failed to move temporary file: {str(e)}")
        return True, None

    except PermissionError:
        return False, 'ファイルの保存権限がありません'
    except OSError as e:
        if hasattr(e, 'winerror') and e.winerror == 112:
            return False, 'ディスク容量が不足しています'
        if e.errno == errno.ENOSPC:
            return False, 'ディスク容量が不足しています'
        return False, f'ファイルの保存/OSエラー: {str(e)}'
    except Exception as e:
        return False, f'ファイルの保存中に予期せぬエラー: {str(e)}'
    finally:
        if temp_file and os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except OSError:
                pass

def confirm_overwrite(filename):
    print(f"ファイル '{filename}' は既に存在します。上書きしますか？ (y/n): ", end='', file=sys.stderr)
    while True:
        try:
            response = input().lower()
            if response in ['y', 'yes']:
                return True
            elif response in ['n', 'no']:
                return False
            else:
                print("無効な入力です。'y' または 'n' を入力してください: ", end='', file=sys.stderr)
        except EOFError:
            print("\n入力が中断されました。", file=sys.stderr)
            return False
        except KeyboardInterrupt:
            print("\n入力が中断されました。", file=sys.stderr)
            return False

def convert_to_sjis(filepath):
    result = {
        'success': False,
        'message': '',
        'original_encoding': '',
        'has_incompatible_chars': False,
        'converted': False,
        'skipped': False
    }

    try:
        if not os.path.exists(filepath):
            result['message'] = 'ファイルが見つかりません'
            return result

        abs_filepath = os.path.abspath(filepath)
        if not os.path.isfile(abs_filepath):
            result['message'] = '有効なファイルパスではありません'
            return result

        encoding, error_msg = detect_encoding(abs_filepath)

        if error_msg:
            if error_msg == "空ファイルです":
                result['success'] = True
                result['message'] = '空ファイルのためスキップ'
                result['skipped'] = True
                result['original_encoding'] = 'N/A'
                return result
            else:
                result['message'] = error_msg
                return result

        if encoding is None:
            result['message'] = '文字コードを検出できませんでした'
            return result

        result['original_encoding'] = encoding

        if encoding == Config.ENCODING_SHIFT_JIS:
            result['success'] = True
            result['message'] = 'SHIFT_JISのためスキップ'
            result['skipped'] = True
            return result

        result['has_incompatible_chars'] = check_sjis_compatibility_stream(abs_filepath, encoding)

        new_filepath, new_filename = generate_output_filename(abs_filepath, result['has_incompatible_chars'])

        if os.path.exists(new_filepath):
            if not confirm_overwrite(new_filename):
                result['message'] = '変換をキャンセルしました (上書きせず)'
                result['skipped'] = True
                return result

        conversion_success, conversion_error = convert_file_stream(abs_filepath, new_filepath, encoding)

        if conversion_success:
            result['success'] = True
            result['message'] = new_filename
            result['converted'] = True
        else:
            result['message'] = conversion_error if conversion_error else 'ファイル変換に失敗しました'

        return result

    except KeyboardInterrupt:
        result['message'] = 'ユーザーによりキャンセルされました'
        return result
    except Exception as e:
        result['message'] = f'予期しないエラー: {str(e)}'
        import traceback
        return result

def format_result_message(result, original_filename):
    if not result['success'] and not result['skipped']:
        return f"{original_filename} → 変換失敗 ({result['message']})"

    if result['skipped']:
        return f"{original_filename} → {result['message']}"

    base_message = f"{original_filename} ({result['original_encoding']}) → {result['message']} ({Config.ENCODING_SHIFT_JIS}へ変換"

    if result['has_incompatible_chars']:
        base_message += "、代替文字に置換あり"

    base_message += ")"
    return base_message

def _display_help_message():
    """使用方法のメッセージを表示する."""
    print("=" * 30)
    print("Shift_JIS変換ツール")
    print("=" * 30)
    print("使用方法:")
    print("1. ファイルの文字コードをShift_JISに変換するツールです。")
    print("   対応文字コード: UTF-8、EUC-JP、ISO-2022-JPなど。")
    print("   対応ファイル: テキストベースのファイル (例: csv, txt, html)。バイナリファイルは不可。")
    print(f"   制限: 最大ファイルサイズ {Config.MAX_FILE_SIZE // (1024*1024)}MB。")
    print("2. ファイルをこの実行ファイルにドラッグ＆ドロップしてください。")
    print("3. 複数ファイルの一括処理も可能です。")
    print("4. 変換後のファイルは元のファイルと同じフォルダに保存されます。")
    print("5. Shift_JISで表現できない文字（例:一部の特殊記号、丸囲み数字など）は")
    print("   代替文字[？]に置換されることがあります。")
    print("6. 変換後のファイル名:")
    print(f"   通常変換:           元のファイル名{Config.SJIS_SUFFIX}.拡張子")
    print(f"   代替文字に置換あり: 元のファイル名{Config.SJISX_SUFFIX}.拡張子")

def _process_files_from_args(filepaths):
    """コマンドライン引数で渡されたファイルを処理し、結果を返す."""
    results = []
    converted_count = 0
    skipped_count = 0
    failed_count = 0
    total_count = len(filepaths)

    print("変換処理を開始します...")
    print("変換後のファイルは、変換前のフォルダに保存されます。\n")

    for i, filepath_arg in enumerate(filepaths, 1):
        try:
            original_filename = os.path.basename(filepath_arg) if filepath_arg else f"ファイル{i}"
            print(f"処理中 ({i}/{total_count}): {original_filename}")

            result = convert_to_sjis(filepath_arg)
            results.append((original_filename, result))

            if result['success']:
                if result.get('skipped', False):
                    skipped_count += 1
                elif result.get('converted', False):
                    converted_count += 1
            else:
                if not result.get('skipped', False):
                    failed_count +=1

        except Exception as e:
            print(f"エラーが発生しました: {e}", file=sys.stderr)
            failed_count += 1

    return results, converted_count, skipped_count, failed_count, total_count

def _display_conversion_summary(results, converted_count, skipped_count, failed_count, total_count):
    """変換結果の要約と詳細を表示する."""
    print(f"\n{'='*50}")
    print("=== 変換結果 ===")

    for fname, res_item in results:
        print(format_result_message(res_item, fname))

    summary_parts = []
    if converted_count > 0:
        summary_parts.append(f"{converted_count}ファイル変換")
    if skipped_count > 0:
        summary_parts.append(f"{skipped_count}ファイルスキップ")
    if failed_count > 0:
        summary_parts.append(f"{failed_count}ファイル失敗")

    summary_str = "、".join(summary_parts) if summary_parts else "処理対象なし"

    print(f"\n処理完了（{total_count}ファイル中 {summary_str}）")
    print(f"{'='*50}")

def main():
    try:
        if len(sys.argv) > 1:
            filepaths = sys.argv[1:]
            results, converted_count, skipped_count, failed_count, total_count = _process_files_from_args(filepaths)
            _display_conversion_summary(results, converted_count, skipped_count, failed_count, total_count)
        else:
            _display_help_message()

    except KeyboardInterrupt:
        print("\nプログラムが中断されました。")
    except Exception as e:
        import traceback
        print(f"\n重大なエラーが発生しました: {str(e)}", file=sys.stderr)
        print(f"{traceback.format_exc()}", file=sys.stderr)

    try:
        input("\nEnterキーを押して終了...")
    except (EOFError, KeyboardInterrupt):
        pass

if __name__ == "__main__":
    main()