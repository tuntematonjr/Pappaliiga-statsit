# Sössö Bot Logo Image

## Required Image File

Place the Sössö bot logo image at:
```
frontend/static/sosso-bot-logo.png
```

## Image Requirements

- **Format**: PNG with transparency (recommended) or JPG
- **Size**: Approximately 400x400 pixels (square format works best)
- **Content**: The logo/avatar for the Sössö Discord bot
- **Background**: Transparent or white background recommended

## After Adding the Image

Once you add `sosso-bot-logo.png` to the `frontend/static/` directory:

1. The image will be accessible at `/static/sosso-bot-logo.png` when the server runs
2. The hero card will display properly on the home page
3. No server restart needed - just refresh the browser

## Customization

If you want to use a different filename or location:

1. Edit `frontend/static/views/HomeView.js` line ~328
2. Change the `logo` property in the `sosso-bot` callout object
3. Update the path to match your new image location

## Discord Bot URLs

Don't forget to update these URLs in `HomeView.js`:

- **primaryHref**: Replace `YOUR_BOT_CLIENT_ID` with actual Discord bot client ID for the OAuth invite link
- **secondaryHref**: Update with the actual documentation/GitHub URL for the bot
