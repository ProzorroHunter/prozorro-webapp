from flask import Flask, request, jsonify
def search():
    tender_id = request.args.get("id")

    if not tender_id:
        return jsonify({"error": "No ID"})

    result = find_tender(tender_id)

    if not result:
        return jsonify({"error": "Not found"})

    return result

if __name__ == "__main__":
    app.run(debug=True)



