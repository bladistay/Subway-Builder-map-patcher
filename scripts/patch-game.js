// language: javascript
    import fs from 'fs';
    import { cpSync, copyFileSync, chmodSync, mkdirSync, existsSync, rmSync, readdirSync, readFileSync, writeFileSync, statSync } from 'fs';
    import path from 'path';
    import { fileURLToPath } from 'url';
    import { execSync } from 'child_process';

    const __dirname = path.dirname(fileURLToPath(import.meta.url));

    const throwError = (msg, err = null) => {
        if (err && err.message) throw new Error(`${msg}: ${err.message}`);
        throw new Error(msg);
    };

    const ensureDir = (p) => { if (!existsSync(p)) mkdirSync(p, { recursive: true }); };

    const gzipFileSync = (fullPath) => {
        try { execSync(`gzip -f "${fullPath}"`); } catch (e) { throwError(`Failed to gzip ${fullPath}`, e); }
    };

    const pathToFileUrl = (p) => `file://${path.resolve(p)}`;

    const loadConfig = async (cfgPath) => {
        try {
            const url = pathToFileUrl(cfgPath);
            const mod = await import(url);
            return mod.default || mod.config || mod;
        } catch (e) {
            throwError('Failed to load config.js', e);
        }
    };

    const findFilesSafe = (root, targetName) => {
        const found = [];
        const stack = [root];
        while (stack.length) {
            const cur = stack.pop();
            let st;
            try { st = statSync(cur); } catch (_) { continue; }
            if (st.isDirectory()) {
                let items;
                try { items = readdirSync(cur); } catch (_) { continue; }
                for (const it of items) stack.push(path.join(cur, it));
            } else {
                if (path.basename(cur).toLowerCase() === targetName.toLowerCase()) found.push(cur);
            }
        }
        return found;
    };

    const main = async () => {
        const workDir = path.resolve(process.cwd(), 'patching_working_directory');
        if (existsSync(workDir)) rmSync(workDir, { recursive: true, force: true });
        ensureDir(workDir);

        const cfgPath = path.resolve(__dirname, '..', 'config.js');
        const config = await loadConfig(cfgPath);
        if (!config || !Array.isArray(config.places)) throwError('Invalid config.places');

        const tileServer = config.tileServerUrl || 'https://api.maptiler.com/tiles/v3/{z}/{x}/{y}.pbf?key=0C77JrjNpG5CBVK3iysK';

        try {
            if (config.platform === 'linux') {
                const appImageSrc = path.resolve(config.subwaybuilderLocation || config.location || '');
                if (!existsSync(appImageSrc)) throwError('AppImage not found');
                const appImageDst = path.join(workDir, 'SB.AppImage');
                copyFileSync(appImageSrc, appImageDst);
                chmodSync(appImageDst, 0o755);
                execSync(`${appImageDst} --appimage-extract`, { cwd: workDir });
            } else if (config.platform === 'windows') {
                let src = path.resolve(config.subwaybuilderLocation || config.location || '');
                if (!existsSync(src)) throwError('Windows install path not found');
                try {
                    const s = statSync(src);
                    if (s.isFile()) {
                        // If user pointed to the exe (or installer), try using its parent folder if it looks like the game install
                        const parent = path.dirname(src);
                        if (existsSync(parent) && statSync(parent).isDirectory()) {
                            const parentFiles = readdirSync(parent).map(f => f.toLowerCase());
                            const looksLikeInstall = parentFiles.includes('resources') || parentFiles.some(f => f.includes('subway') || f.endsWith('.exe'));
                            if (looksLikeInstall) {
                                src = parent;
                            } else {
                                throwError(`Windows install path points to a file (${src}). Set config.subwaybuilderLocation to the installed game folder (e.g. "C:\\\\Program Files\\\\Subway Builder").`);
                            }
                        } else {
                            throwError(`Windows install path points to a file (${src}) and its parent is not a usable folder.`);
                        }
                    }
                } catch (e) {
                    throwError('Failed to stat Windows install path', e);
                }
                // copy directory content into working dir
                cpSync(src, path.join(workDir, 'squashfs-root'), { recursive: true });
            } else if (config.platform === 'macos') {
                const src = path.resolve(config.subwaybuilderLocation || config.location || '');
                if (!existsSync(src)) throwError('Mac app path not found');
                cpSync(path.join(src, 'Contents'), path.join(workDir, 'squashfs-root'), { recursive: true });
                const resOld = path.join(workDir, 'squashfs-root', 'Resources');
                const resNew = path.join(workDir, 'squashfs-root', 'resources');
                if (existsSync(resOld) && !existsSync(resNew)) {
                    try { fs.renameSync(resOld, resNew); } catch (_) {}
                }
            } else {
                throwError('Unsupported platform in config.platform');
            }
        } catch (e) {
            throwError('Failed to copy/extract game files', e);
        }

        const squashRoot = path.join(workDir, 'squashfs-root');

        // If the copy produced a non-directory (e.g. user pointed to a single file), give a clearer message
        if (existsSync(squashRoot) && !statSync(squashRoot).isDirectory()) {
            throwError(`Expected "${squashRoot}" to be a directory but it's a file. Make sure config.subwaybuilderLocation points to the installed game folder (not an installer/exe).`);
        }

        // try common locations first, then fall back to searching inside the extracted tree
        let appAsarPath = path.join(squashRoot, 'resources', 'app.asar');
        if (!existsSync(appAsarPath)) {
            const alt = path.join(squashRoot, 'App', 'resources', 'app.asar');
            if (existsSync(alt)) appAsarPath = alt;
        }

        // fallback: search the whole workDir for any app.asar files (safe traversal)
        if (!existsSync(appAsarPath)) {
            const found = findFilesSafe(workDir, 'app.asar');
            if (found.length > 0) {
                appAsarPath = found[0];
                console.warn(`Found app.asar at ${appAsarPath}`);
            }
        }

        if (!existsSync(appAsarPath)) {
            // helpful diagnostic listing to help user fix config.path
            let listing = '';
            try {
                const top = readdirSync(workDir);
                listing = top.join(', ');
            } catch (_) { listing = '(could not list workDir)'; }
            throwError(`app.asar not found in extracted game. Searched: ${path.join(squashRoot, 'resources', 'app.asar')} and ${path.join(squashRoot, 'App', 'resources', 'app.asar')}. Work dir contents: ${listing}`);
        }

        const extractedAsarDir = path.join(workDir, 'extracted-asar');
        ensureDir(extractedAsarDir);
        try { execSync(`npx @electron/asar extract "${appAsarPath}" "${extractedAsarDir}"`); } catch (e) { throwError('Failed to extract app.asar', e); }

        const publicDir = path.join(extractedAsarDir, 'dist', 'renderer', 'public');
        if (existsSync(publicDir) && statSync(publicDir).isDirectory()) {
            const publicFiles = readdirSync(publicDir);
            const indexFiles = publicFiles.filter(f => f.startsWith('index-') && f.endsWith('.js'));
            const gameMainFiles = publicFiles.filter(f => f.startsWith('GameMain-') && f.endsWith('.js'));

            if (indexFiles.length > 0) {
                const indexPath = path.join(publicDir, indexFiles[0]);
                let indexTxt = readFileSync(indexPath, 'utf8');

                const citiesPos = indexTxt.indexOf('const cities');
                if (citiesPos !== -1) {
                    const arrStart = indexTxt.indexOf('[', citiesPos);
                    let pos = arrStart;
                    let depth = 0;
                    let arrEnd = -1;
                    while (pos < indexTxt.length) {
                        if (indexTxt[pos] === '[') depth++;
                        else if (indexTxt[pos] === ']') {
                            depth--;
                            if (depth === 0) { arrEnd = pos; break; }
                        }
                        pos++;
                    }
                    if (arrStart !== -1 && arrEnd !== -1) {
                        const existingArrayStr = indexTxt.substring(arrStart, arrEnd + 1);
                        const newEntries = config.places.map(place => JSON.stringify({
                            name: place.name,
                            code: place.code,
                            description: place.description,
                            population: place.population,
                            initialViewState: {
                                zoom: 13.5,
                                latitude: (place.bbox[1] + place.bbox[3]) / 2,
                                longitude: (place.bbox[0] + place.bbox[2]) / 2,
                                bearing: 0
                            }
                        })).join(',');

                        const trimmed = existingArrayStr.trim();
                        const augmented = (trimmed === '[]') ? `[${newEntries}]` : trimmed.slice(0, -1) + ',' + newEntries + ']';
                        indexTxt = indexTxt.slice(0, arrStart) + augmented + indexTxt.slice(arrEnd + 1);
                        writeFileSync(indexPath, indexTxt, 'utf8');

                        const placeholderSrc = path.resolve(__dirname, '..', 'placeholder_mapimage.svg');
                        const cityMapsDir = path.join(publicDir, '..', 'city-maps');
                        ensureDir(cityMapsDir);
                        if (existsSync(placeholderSrc)) {
                            for (const place of config.places) {
                                try { copyFileSync(placeholderSrc, path.join(cityMapsDir, `${place.code.toLowerCase()}.svg`)); } catch (_) {}
                            }
                        }
                    }
                }
            }

            if (gameMainFiles.length > 0) {
                const gmPath = path.join(publicDir, gameMainFiles[0]);
                let gmTxt = readFileSync(gmPath, 'utf8');

                const sourcesKey = 'const sources =';
                const sourcesPos = gmTxt.indexOf(sourcesKey);
                const layersKey = 'const layers';
                if (sourcesPos !== -1) {
                    const startOfSources = gmTxt.indexOf('{', sourcesPos);
                    const endOfSources = gmTxt.indexOf(layersKey, startOfSources);
                    if (startOfSources !== -1 && endOfSources !== -1) {
                        const existingSources = gmTxt.substring(startOfSources, endOfSources);
                        const newTilesArr = JSON.stringify([tileServer, tileServer, tileServer, tileServer]);
                        let newSources = existingSources.replace(/\[.*?tilesUrl.*?\]/s, newTilesArr);
                        newSources = newSources.replaceAll('maxzoom: 16', 'maxzoom: 15');
                        gmTxt = gmTxt.slice(0, startOfSources) + newSources + gmTxt.slice(endOfSources);
                        writeFileSync(gmPath, gmTxt, 'utf8');
                    }
                }
            }

            const tileServerDomain = new URL(tileServer).hostname;

            const mainJsPath = path.join(extractedAsarDir, 'dist', 'main', 'main.js');
            let mainJsTxt = readFileSync(mainJsPath, 'utf8');
            writeFileSync(path.join(workDir, 'main_js_patching.txt'), mainJsTxt, 'utf8');

            mainJsTxt = mainJsTxt.replace(/\/maptiler\\.com\/i,/g, `/example.com/i,`);
            mainJsTxt = mainJsTxt.replace("/^https:\\/\\/ctiles\\.subwaybuilder\\.com/", `/https:\\/\\/${tileServerDomain}/i, /^https:\\/\\/ctiles\\.subwaybuilder\\.com/`);
            writeFileSync(mainJsPath, mainJsTxt, 'utf8');
            writeFileSync(path.join(workDir, 'main_js_patching.txt'), mainJsTxt, 'utf8');

            if (gameMainFiles.length > 0) {
                try {
                    const gmPath2 = path.join(publicDir, gameMainFiles[0]);
                    let gmTxt2 = readFileSync(gmPath2, 'utf8');

                    const newTilesArr2 = JSON.stringify([tileServer, tileServer, tileServer, tileServer]);
                    gmTxt2 = gmTxt2.replace(/\[(tilesUrl|foundationTilesUrl)\]/g, newTilesArr2);
                    gmTxt2 = gmTxt2.replaceAll('maxzoom: 16', 'maxzoom: 15');

                    const parksBlockStart = gmTxt2.indexOf('id: "parks-large"');
                    if (parksBlockStart !== -1) {
                        let blockEnd = parksBlockStart;
                        let braceCount = 0;
                        let inBlock = false;
                        for (let i = parksBlockStart; i < gmTxt2.length; i++) {
                            if (gmTxt2[i] === '{') { braceCount++; inBlock = true; }
                            else if (gmTxt2[i] === '}') { braceCount--; if (inBlock && braceCount === 0) { blockEnd = i + 1; break; } }
                        }
                        if (blockEnd > parksBlockStart) {
                            const oldBlock = gmTxt2.substring(parksBlockStart, blockEnd);
                            let newBlock = oldBlock
                                .replace('"source-layer": "parks"', '"source-layer": "landcover"')
                                .replace(/filter:\s*\[">=",\s*\["get",\s*"area"\],\s*1e5\],/, '')
                                .replace(/"fill-extrusion-height":\s*[^,]+/, '"fill-extrusion-height": 0.1');
                            gmTxt2 = gmTxt2.substring(0, parksBlockStart) + newBlock + gmTxt2.substring(blockEnd);
                        }
                    }

                    const waterBlockStart = gmTxt2.indexOf('id: "water"');
                    if (waterBlockStart !== -1) {
                        let blockEnd = waterBlockStart;
                        let braceCount = 0;
                        let inBlock = false;
                        for (let i = waterBlockStart; i < gmTxt2.length; i++) {
                            if (gmTxt2[i] === '{') { braceCount++; inBlock = true; }
                            else if (gmTxt2[i] === '}') { braceCount--; if (inBlock && braceCount === 0) { blockEnd = i + 1; break; } }
                        }
                        if (blockEnd > waterBlockStart) {
                            const oldBlock = gmTxt2.substring(waterBlockStart, blockEnd);
                            let newBlock = oldBlock.replace(/"fill-extrusion-height":\s*[^,]+/, '"fill-extrusion-height": 0.2');
                            gmTxt2 = gmTxt2.substring(0, waterBlockStart) + newBlock + gmTxt2.substring(blockEnd);
                        }
                    }

                    gmTxt2 = gmTxt2.replace('"source-layer": "buildings"', '"source-layer": "building"');
                    gmTxt2 = gmTxt2.replaceAll('"source-layer": "airports"', '"source-layer": "aviation"');

                    writeFileSync(gmPath2, gmTxt2, 'utf8');
                } catch (e) {}
            }
        }

        try { execSync(`npx @electron/asar pack "${extractedAsarDir}" "${appAsarPath}"`); } catch (e) { throwError('Failed to pack app.asar', e); }

        const dataRoot = path.join(squashRoot, 'resources', 'data');
        ensureDir(dataRoot);
        for (const place of config.places) {
            const code = place.code;
            const srcDir = path.resolve(process.cwd(), 'processed-data', code);
            if (!existsSync(srcDir) || !statSync(srcDir).isDirectory()) throwError(`Prepared data missing for ${code}`);
            const destDir = path.join(dataRoot, code);
            if (existsSync(destDir)) rmSync(destDir, { recursive: true, force: true });
            cpSync(srcDir, destDir, { recursive: true });

            const files = readdirSync(destDir);
            const hasBuildings = files.some(f => f.startsWith('buildings') && (f.endsWith('.json') || f.endsWith('.json.gz')));
            const hasDemand = files.some(f => f.startsWith('demand') && (f.endsWith('.json') || f.endsWith('.json.gz')));
            const hasRoads = files.some(f => f.startsWith('roads') && (f.endsWith('.geojson') || f.endsWith('.geojson.gz')));
            if (!hasBuildings || !hasDemand || !hasRoads) throwError(`Required map files missing for ${code}`);

            for (const f of files) {
                if (f.endsWith('.gz')) continue;
                gzipFileSync(path.join(destDir, f));
            }
        }

        const buildsDir = path.resolve(__dirname, '..', 'builds');
        ensureDir(buildsDir);

        if (config.platform === 'linux') {
            const appimagetool = path.resolve(__dirname, '..', 'appimagetool.AppImage');
            const appDir = path.join(workDir, 'sbp.AppDir');
            if (existsSync(path.join(workDir, 'squashfs-root')) && !existsSync(appDir)) {
                try { fs.renameSync(path.join(workDir, 'squashfs-root'), appDir); } catch (_) {}
            }
            if (existsSync(appimagetool)) {
                try { chmodSync(appimagetool, 0o755); execSync(`"${appimagetool}" "${appDir}"`, { cwd: path.resolve(__dirname, '..') }); } catch (_) {}
                const rootFiles = readdirSync(path.resolve(__dirname, '..'));
                for (const f of rootFiles) if (f.endsWith('.AppImage')) try { copyFileSync(path.join(path.resolve(__dirname, '..'), f), path.join(buildsDir, f)); } catch (_) {}
            } else {
                try { execSync(`tar -czf "${path.join(buildsDir, 'sbp_AppDir.tar.gz')}" -C "${workDir}" sbp.AppDir`); } catch (_) {}
            }
        } else if (config.platform === 'windows') {
            const target = path.resolve(__dirname, '..', 'Subway_Builder');
            if (existsSync(target)) rmSync(target, { recursive: true, force: true });
            try { cpSync(path.join(workDir, 'squashfs-root'), target, { recursive: true }); } catch (e) { throwError('Failed to copy Windows build folder', e); }

            const outZip = path.join(buildsDir, 'Subway_Builder_windows.zip');
            try { execSync(`powershell -NoProfile -Command "Compress-Archive -Path '${target}\\\\*' -DestinationPath '${outZip}' -Force"`); } catch (_) {
                try { execSync(`zip -r -q "${outZip}" "${target}"`); } catch (_) {}
            }

            try {
                execSync('7z -h', { stdio: 'ignore' });

                const archive7z = path.join(buildsDir, 'Subway_Builder_windows.7z');
                try { execSync(`7z a -t7z -mx=9 "${archive7z}" "${target}"`, { cwd: buildsDir }); } catch (_) { throw new Error('7z failed to create archive'); }

                const sfxCandidates = [
                    '/usr/lib/p7zip/7z.sfx',
                    '/usr/local/lib/p7zip/7z.sfx',
                    '/usr/lib/7zip/7z.sfx',
                    '/usr/local/share/p7zip/7z.sfx',
                ];
                let sfxModule = null;
                for (const cand of sfxCandidates) {
                    if (existsSync(cand)) { sfxModule = cand; break; }
                }
            } catch (e) {

            }
        } else if (config.platform === 'macos') {
            const originalAppPath = config.location || '/Applications/Subway Builder.app';
            const patchedAppPath = path.resolve(__dirname, '..', 'Subway_Builder_Patched.app');
            try { execSync(`ditto "${originalAppPath}" "${patchedAppPath}"`); } catch (e) { throwError('Failed to copy original .app for patching', e); }
            const packedAsarPath = path.join(workDir, 'squashfs-root', 'resources', 'app.asar');
            const targetAsarPath = path.join(patchedAppPath, 'Contents', 'Resources', 'app.asar');
            try { copyFileSync(packedAsarPath, targetAsarPath); } catch (e) { throwError('Failed to write app.asar into patched app', e); }
            for (const place of config.places) {
                const src = path.join(workDir, 'squashfs-root', 'resources', 'data', place.code);
                const dstRoot = path.join(patchedAppPath, 'Contents', 'Resources', 'data');
                ensureDir(dstRoot);
                const dst = path.join(dstRoot, place.code);
                if (existsSync(dst)) rmSync(dst, { recursive: true, force: true });
                try { cpSync(src, dst, { recursive: true }); } catch (e) { throwError(`Failed to copy data for ${place.code} into patched app`, e); }
            }
            try { execSync(`xattr -cr "${patchedAppPath}"`); } catch (_) {}
            try { execSync(`codesign --force --deep -s - "${patchedAppPath}"`); } catch (_) {}
            const dmgOut = path.join(buildsDir, 'Subway_Builder_macos.dmg');
            try { execSync(`hdiutil create -volname "Subway Builder" -srcfolder "${patchedAppPath}" -ov -format UDZO "${dmgOut}"`); } catch (_) {}
        }
    };

    await main();